use heed::Result as ZResult;
use heed::types::{OwnedType, Str};

use crate::database::MainT;
use crate::DocumentId;
use crate::store::BEU64;

#[derive(Copy, Clone)]
pub struct DocumentIdToUserId {
    pub(crate) document_id_to_user_id: heed::Database<OwnedType<BEU64>, Str>,
}

impl DocumentIdToUserId {
    pub fn put_document_id(
        self,
        writer: &mut heed::RwTxn<MainT>,
        document_id: DocumentId,
        user_id: &str,
    ) -> ZResult<()>
    {
        let id = BEU64::new(document_id.0);
        self.document_id_to_user_id.put(writer, &id, user_id)
    }

    pub fn del_document_id(
        self,
        writer: &mut heed::RwTxn<MainT>,
        document_id: DocumentId,
    ) -> ZResult<bool>
    {
        let id = BEU64::new(document_id.0);
        self.document_id_to_user_id.delete(writer, &id)
    }

    pub fn clear(self, writer: &mut heed::RwTxn<MainT>) -> ZResult<()> {
        self.document_id_to_user_id.clear(writer)
    }

    pub fn document_id<'txn>(
        self,
        reader: &'txn heed::RoTxn<MainT>,
        document_id: DocumentId,
    ) -> ZResult<Option<&'txn str>>
    {
        let id = BEU64::new(document_id.0);
        self.document_id_to_user_id.get(reader, &id)
    }

    pub fn next_available_document_id(self, reader: &heed::RoTxn<MainT>) -> ZResult<DocumentId> {
        let mut previous: Option<u64> = None;

        for result in self.document_id_to_user_id.iter(reader)? {
            let (document_id, _) = result?;
            let document_id = document_id.get();

            if let Some(previous) = previous {
                let next = previous.checked_add(1).unwrap();
                if next != document_id {
                    return Ok(DocumentId(next));
                }
            }

            previous = Some(document_id);
        }

        match previous {
            Some(previous) => Ok(DocumentId(previous.checked_add(1).unwrap())),
            None => Ok(DocumentId(0)),
        }
    }

    pub fn next_available_documents_ids(
        self,
        reader: &heed::RoTxn<MainT>,
        count: usize,
    ) -> ZResult<Vec<DocumentId>>
    {
        let mut documents_ids = Vec::with_capacity(count);
        let mut previous = None;

        for result in self.document_id_to_user_id.iter(reader)? {
            let (current, _) = result?;
            let current = current.get();

            if let Some(previous) = previous {
                let remaining = count - documents_ids.len();
                let iter = ((previous + 1)..current).map(DocumentId).take(remaining);
                documents_ids.extend(iter);
            }

            previous = Some(current);
        }

        if documents_ids.len() != count {
            let remaining = count - documents_ids.len();
            let start = previous.map(|x| x + 1).unwrap_or(0);
            let iter = (start..).map(DocumentId).take(remaining);
            documents_ids.extend(iter);
        }

        Ok(documents_ids)
    }
}
