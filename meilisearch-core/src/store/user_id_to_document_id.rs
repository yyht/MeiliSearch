use heed::Result as ZResult;
use heed::types::{OwnedType, Str};

use crate::database::MainT;
use crate::DocumentId;

#[derive(Copy, Clone)]
pub struct UserIdToDocumentId {
    pub(crate) user_id_to_document_id: heed::Database<Str, OwnedType<u64>>,
}

impl UserIdToDocumentId {
    pub fn put_user_id(
        self,
        writer: &mut heed::RwTxn<MainT>,
        user_id: &str,
        document_id: DocumentId,
    ) -> ZResult<()>
    {
        self.user_id_to_document_id.put(writer, &user_id, &document_id.0)
    }

    pub fn del_user_id(
        self,
        writer: &mut heed::RwTxn<MainT>,
        user_id: &str,
    ) -> ZResult<bool>
    {
        self.user_id_to_document_id.delete(writer, &user_id)
    }

    pub fn clear(self, writer: &mut heed::RwTxn<MainT>) -> ZResult<()> {
        self.user_id_to_document_id.clear(writer)
    }

    pub fn document_id<'txn>(
        self,
        reader: &'txn heed::RoTxn<MainT>,
        user_id: &str,
    ) -> ZResult<Option<DocumentId>>
    {
        match self.user_id_to_document_id.get(reader, user_id)? {
            Some(id) => Ok(Some(DocumentId(id))),
            None => Ok(None),
        }
    }

    pub fn iter(self, reader: &heed::RoTxn<MainT>) -> ZResult<UserIdToDocumentIdIter> {
        Ok(UserIdToDocumentIdIter { iter: self.user_id_to_document_id.iter(reader)? })
    }
}

pub struct UserIdToDocumentIdIter<'txn> {
    iter: heed::RoIter<'txn, Str, OwnedType<u64>>,
}

impl<'txn> Iterator for UserIdToDocumentIdIter<'txn> {
    type Item = ZResult<(&'txn str, DocumentId)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok((user_id, document_id))) => Some(Ok((user_id, DocumentId(document_id)))),
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}
