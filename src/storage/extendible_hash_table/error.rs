use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExtendibleHashTableError {
    #[error("Can't grow hash table directory: Max size reached.")]
    DirectoryMaxSizeReached,
    #[error("Can't load directory by page id.")]
    NoDirectoryForPageId,
    #[error("Can't load bucket by page id.")]
    NoBucketForPageId,
    #[error("unknown database error")]
    Unknown,
}