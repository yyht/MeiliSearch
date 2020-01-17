use std::collections::{HashMap, BTreeMap};

use fst::{set::OpBuilder, SetBuilder};
use itertools::{merge_join_by, EitherOrBoth};
use sdset::{duo::Union, SetOperation};
use serde::{Deserialize, Serialize};

use crate::database::{MainT, UpdateT};
use crate::database::{UpdateEvent, UpdateEventsEmitter};
use crate::raw_indexer::RawIndexer;
use crate::serde::{extract_user_id, serialize_value, Deserializer, Serializer};
use crate::store;
use crate::update::{apply_documents_deletion, compute_short_prefixes, next_update_id};
use crate::update::{Update, AlterDocumentIds};
use crate::{Error, MResult, RankedMap, DocumentId};

pub struct DocumentsAddition<D> {
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    updates_notifier: UpdateEventsEmitter,
    documents: Vec<D>,
    is_partial: bool,
}

impl<D> DocumentsAddition<D> {
    pub fn new(
        updates_store: store::Updates,
        updates_results_store: store::UpdatesResults,
        updates_notifier: UpdateEventsEmitter,
    ) -> DocumentsAddition<D> {
        DocumentsAddition {
            updates_store,
            updates_results_store,
            updates_notifier,
            documents: Vec::new(),
            is_partial: false,
        }
    }

    pub fn new_partial(
        updates_store: store::Updates,
        updates_results_store: store::UpdatesResults,
        updates_notifier: UpdateEventsEmitter,
    ) -> DocumentsAddition<D> {
        DocumentsAddition {
            updates_store,
            updates_results_store,
            updates_notifier,
            documents: Vec::new(),
            is_partial: true,
        }
    }

    pub fn update_document(&mut self, document: D) {
        self.documents.push(document);
    }

    pub fn finalize(self, writer: &mut heed::RwTxn<UpdateT>) -> MResult<u64>
    where
        D: serde::Serialize,
    {
        let _ = self.updates_notifier.send(UpdateEvent::NewUpdate);
        let update_id = push_documents_addition(
            writer,
            self.updates_store,
            self.updates_results_store,
            self.documents,
            self.is_partial,
        )?;
        Ok(update_id)
    }
}

impl<D> Extend<D> for DocumentsAddition<D> {
    fn extend<T: IntoIterator<Item = D>>(&mut self, iter: T) {
        self.documents.extend(iter)
    }
}

pub fn push_documents_addition<D: serde::Serialize>(
    writer: &mut heed::RwTxn<UpdateT>,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    addition: Vec<D>,
    is_partial: bool,
) -> MResult<u64> {
    let mut values = Vec::with_capacity(addition.len());
    for add in addition {
        let vec = serde_json::to_vec(&add)?;
        let add = serde_json::from_slice(&vec)?;
        values.push(add);
    }

    let last_update_id = next_update_id(writer, updates_store, updates_results_store)?;

    let update = if is_partial {
        Update::documents_partial(values)
    } else {
        Update::documents_addition(values)
    };

    updates_store.put_update(writer, last_update_id, &update)?;

    Ok(last_update_id)
}

pub fn apply_documents_addition<'a, 'b>(
    writer: &'a mut heed::RwTxn<'b, MainT>,
    index: &store::Index,
    addition: Vec<HashMap<String, serde_json::Value>>,
) -> MResult<()> {
    let mut documents_additions = BTreeMap::new();

    let schema = match index.main.schema(writer)? {
        Some(schema) => schema,
        None => return Err(Error::SchemaMissing),
    };

    let identifier = schema.identifier_name();

    // 1. store documents ids for future deletion
    for document in addition {
        let user_id = match extract_user_id(identifier, &document)? {
            Some(id) => id,
            None => return Err(Error::MissingDocumentId),
        };

        documents_additions.insert(user_id, document);
    }

    // 2. remove the documents posting lists
    let number_of_inserted_documents = documents_additions.len();
    let documents_ids = documents_additions.iter().map(|(id, _)| id.clone()).collect();
    apply_documents_deletion(writer, index, documents_ids, AlterDocumentIds::Keep)?;

    let mut ranked_map = match index.main.ranked_map(writer)? {
        Some(ranked_map) => ranked_map,
        None => RankedMap::default(),
    };

    let stop_words = match index.main.stop_words_fst(writer)? {
        Some(stop_words) => stop_words,
        None => fst::Set::default(),
    };

    // 3. compute the documents ids
    let identified_documents = identify_documents(writer, index, documents_additions)?;

    // 4. index the documents fields in the stores
    let mut indexer = RawIndexer::new(stop_words);

    for (document_id, document) in identified_documents {
        let serializer = Serializer {
            txn: writer,
            schema: &schema,
            document_store: index.documents_fields,
            document_fields_counts: index.documents_fields_counts,
            indexer: &mut indexer,
            ranked_map: &mut ranked_map,
            document_id,
        };

        document.serialize(serializer)?;
    }

    write_documents_addition_index(
        writer,
        index,
        &ranked_map,
        number_of_inserted_documents,
        indexer,
    )?;

    compute_short_prefixes(writer, index)?;

    Ok(())
}

pub fn apply_documents_partial_addition<'a, 'b>(
    writer: &'a mut heed::RwTxn<'b, MainT>,
    index: &store::Index,
    addition: Vec<HashMap<String, serde_json::Value>>,
) -> MResult<()> {
    let mut documents_additions = BTreeMap::new();

    let schema = match index.main.schema(writer)? {
        Some(schema) => schema,
        None => return Err(Error::SchemaMissing),
    };

    let identifier = schema.identifier_name();

    // 1. store documents ids for future deletion
    for mut document in addition {
        let user_id = match extract_user_id(identifier, &document)? {
            Some(id) => id,
            None => return Err(Error::MissingDocumentId),
        };

        if let Some(document_id) = index.user_id_to_document_id.document_id(writer, &user_id)? {
            let mut deserializer = Deserializer {
                document_id,
                reader: writer,
                documents_fields: index.documents_fields,
                schema: &schema,
                attributes: None,
            };

            // retrieve the old document and
            // update the new one with missing keys found in the old one
            let result = Option::<HashMap<String, serde_json::Value>>::deserialize(&mut deserializer)?;
            if let Some(old_document) = result {
                for (key, value) in old_document {
                    document.entry(key).or_insert(value);
                }
            }
        }

        documents_additions.insert(user_id, document);
    }

    // 2. remove the documents posting lists
    let number_of_inserted_documents = documents_additions.len();
    let documents_ids = documents_additions.iter().map(|(id, _)| id.clone()).collect();
    apply_documents_deletion(writer, index, documents_ids, AlterDocumentIds::Keep)?;

    let mut ranked_map = match index.main.ranked_map(writer)? {
        Some(ranked_map) => ranked_map,
        None => RankedMap::default(),
    };

    let stop_words = match index.main.stop_words_fst(writer)? {
        Some(stop_words) => stop_words,
        None => fst::Set::default(),
    };

    // 3. compute the documents ids
    let identified_documents = identify_documents(writer, index, documents_additions)?;

    // 4. index the documents fields in the stores
    let mut indexer = RawIndexer::new(stop_words);

    for (document_id, document) in identified_documents {
        let serializer = Serializer {
            txn: writer,
            schema: &schema,
            document_store: index.documents_fields,
            document_fields_counts: index.documents_fields_counts,
            indexer: &mut indexer,
            ranked_map: &mut ranked_map,
            document_id,
        };

        document.serialize(serializer)?;
    }

    write_documents_addition_index(
        writer,
        index,
        &ranked_map,
        number_of_inserted_documents,
        indexer,
    )?;

    compute_short_prefixes(writer, index)?;

    Ok(())
}

fn identify_documents<'a, 'b>(
    writer: &'a mut heed::RwTxn<'b, MainT>,
    index: &store::Index,
    documents_additions: BTreeMap<String, HashMap<String, serde_json::Value>>,
) -> MResult<HashMap<DocumentId, HashMap<String, serde_json::Value>>>
{
    // 3. compute the documents ids
    let known_documents_ids = index.user_id_to_document_id.iter(writer)?;
    let known_documents_ids: Result<Vec<(&str, DocumentId)>, _> = known_documents_ids.collect();
    let known_documents_ids = known_documents_ids?;

    let documents_ids = documents_additions.into_iter();
    let merge = merge_join_by(known_documents_ids, documents_ids, |(a, _), (b, _)| a.cmp(&b.as_str()));

    let mut identified_documents = HashMap::new();
    let mut non_identified_documents = Vec::new();
    for eob in merge {
        match eob {
            EitherOrBoth::Left(_) => (),
            EitherOrBoth::Both((_, id), (_, doc)) => { identified_documents.insert(id, doc); },
            EitherOrBoth::Right((id, doc)) => non_identified_documents.push((id, doc)),
        }
    }

    // 4. compute the documents ids for the unknown documents
    let remaining = non_identified_documents.len();
    let available_ids = index.document_id_to_user_id.next_available_documents_ids(writer, remaining)?;

    for (document_id, (user_id, doc)) in available_ids.into_iter().zip(non_identified_documents) {
        index.user_id_to_document_id.put_user_id(writer, &user_id, document_id)?;
        index.document_id_to_user_id.put_document_id(writer, document_id, &user_id)?;
        identified_documents.insert(document_id, doc);
    }

    Ok(identified_documents)
}

pub fn reindex_all_documents(writer: &mut heed::RwTxn<MainT>, index: &store::Index) -> MResult<()> {
    let schema = match index.main.schema(writer)? {
        Some(schema) => schema,
        None => return Err(Error::SchemaMissing),
    };

    let mut ranked_map = RankedMap::default();

    // 1. retrieve all documents ids
    let mut documents_ids_to_reindex = Vec::new();
    for result in index.user_id_to_document_id.iter(writer)? {
        let (_, document_id) = result?;
        documents_ids_to_reindex.push(document_id);
    }

    // 2. remove the documents posting lists
    index.main.put_words_fst(writer, &fst::Set::default())?;
    index.main.put_ranked_map(writer, &ranked_map)?;
    index.main.put_number_of_documents(writer, |_| 0)?;
    index.documents_fields_counts.clear(writer)?;
    index.postings_lists.clear(writer)?;
    index.docs_words.clear(writer)?;

    // 3. re-index chunks of documents (otherwise we make the borrow checker unhappy)
    for documents_ids in documents_ids_to_reindex.chunks(100) {
        let stop_words = match index.main.stop_words_fst(writer)? {
            Some(stop_words) => stop_words,
            None => fst::Set::default(),
        };

        let number_of_inserted_documents = documents_ids.len();
        let mut indexer = RawIndexer::new(stop_words);
        let mut ram_store = HashMap::new();

        for document_id in documents_ids {
            for result in index.documents_fields.document_fields(writer, *document_id)? {
                let (attr, bytes) = result?;
                let value: serde_json::Value = serde_json::from_slice(bytes)?;
                ram_store.insert((document_id, attr), value);
            }

            for ((docid, attr), value) in ram_store.drain() {
                serialize_value(
                    writer,
                    attr,
                    schema.props(attr),
                    *docid,
                    index.documents_fields,
                    index.documents_fields_counts,
                    &mut indexer,
                    &mut ranked_map,
                    &value,
                )?;
            }
        }

        // 4. write the new index in the main store
        write_documents_addition_index(
            writer,
            index,
            &ranked_map,
            number_of_inserted_documents,
            indexer,
        )?;
    }

    compute_short_prefixes(writer, index)?;

    Ok(())
}

pub fn write_documents_addition_index(
    writer: &mut heed::RwTxn<MainT>,
    index: &store::Index,
    ranked_map: &RankedMap,
    number_of_inserted_documents: usize,
    indexer: RawIndexer,
) -> MResult<()> {
    let indexed = indexer.build();
    let mut delta_words_builder = SetBuilder::memory();

    for (word, delta_set) in indexed.words_doc_indexes {
        delta_words_builder.insert(&word).unwrap();

        let set = match index.postings_lists.postings_list(writer, &word)? {
            Some(postings) => Union::new(&postings.matches, &delta_set).into_set_buf(),
            None => delta_set,
        };

        index.postings_lists.put_postings_list(writer, &word, &set)?;
    }

    for (id, words) in indexed.docs_words {
        index.docs_words.put_doc_words(writer, id, &words)?;
    }

    let delta_words = delta_words_builder
        .into_inner()
        .and_then(fst::Set::from_bytes)
        .unwrap();

    let words = match index.main.words_fst(writer)? {
        Some(words) => {
            let op = OpBuilder::new()
                .add(words.stream())
                .add(delta_words.stream())
                .r#union();

            let mut words_builder = SetBuilder::memory();
            words_builder.extend_stream(op).unwrap();
            words_builder
                .into_inner()
                .and_then(fst::Set::from_bytes)
                .unwrap()
        }
        None => delta_words,
    };

    index.main.put_words_fst(writer, &words)?;
    index.main.put_ranked_map(writer, ranked_map)?;
    index.main.put_number_of_documents(writer, |old| old + number_of_inserted_documents as u64)?;

    compute_short_prefixes(writer, index)?;

    Ok(())
}
