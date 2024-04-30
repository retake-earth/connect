use datafusion::datasource::file_format::{
    avro::AvroFormat, csv::CsvFormat, json::JsonFormat, parquet::ParquetFormat,
};
use datafusion::datasource::listing::ListingOptions;
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct FileExtension(pub String);

pub enum TableFormat {
    None,
    Delta,
}

impl TableFormat {
    pub fn as_str(&self) -> &str {
        match self {
            Self::None => "",
            Self::Delta => "delta",
        }
    }

    pub fn from(format: &str) -> Self {
        match format {
            "" => Self::None,
            "delta" => Self::Delta,
            _ => Self::None,
        }
    }

    pub fn iter() -> impl Iterator<Item = Self> {
        [Self::None, Self::Delta].into_iter()
    }
}

impl TryFrom<FileExtension> for ListingOptions {
    type Error = FormatError;

    fn try_from(format: FileExtension) -> Result<Self, FormatError> {
        let FileExtension(format) = format;

        let listing_options = match format.to_lowercase().as_str() {
            "avro" => ListingOptions::new(Arc::new(AvroFormat)).with_file_extension(".avro"),
            "csv" => {
                ListingOptions::new(Arc::new(CsvFormat::default())).with_file_extension(".csv")
            }
            "json" => {
                ListingOptions::new(Arc::new(JsonFormat::default())).with_file_extension(".json")
            }
            "parquet" => ListingOptions::new(Arc::new(ParquetFormat::default()))
                .with_file_extension(".parquet"),
            unsupported => return Err(FormatError::InvalidFileFormat(unsupported.to_string())),
        };

        Ok(listing_options)
    }
}

#[derive(Error, Debug)]
pub enum FormatError {
    #[error("Invalid format {0}. Options are avro, csv, json, and parquet.")]
    InvalidFileFormat(String),
}
