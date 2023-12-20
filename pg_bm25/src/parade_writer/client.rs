use crate::WriterInitError;
use crate::{
    json::builder::JsonBuilder,
    parade_writer::{ParadeWriterRequest, ParadeWriterResponse},
};
use pgrx::{log, PGRXSharedMemory};
use std::{error::Error, net::SocketAddr};

#[derive(Copy, Clone, Default)]
pub struct ParadeWriterClient {
    addr: Option<SocketAddr>,
    error: Option<WriterInitError>,
}

impl ParadeWriterClient {
    pub fn set_addr(&mut self, addr: SocketAddr) {
        self.addr = Some(addr);
    }

    pub fn set_error(&mut self, err: WriterInitError) {
        self.error = Some(err);
    }

    fn send_request(
        &self,
        request: ParadeWriterRequest,
    ) -> Result<ParadeWriterResponse, Box<dyn Error>> {
        let addr = match self.addr {
            // If there's no addr, the server hasn't started yet.
            // We won't send the shutdown request,but it is up to the insert worker
            // to handle this case by checking for SIGTERM right before starting its server.
            None => match request {
                ParadeWriterRequest::Shutdown => {
                    log!("pg_bm25 shutdown worker skipped sending signal to insert worker");
                    return Ok(ParadeWriterResponse::Ok);
                }
                // If it wasn't a shutdown request, then we have a problem if the server has not
                // been started. Return an error.
                req => {
                    return Err(format!(
                        "pg_bm25 writer not yet initialized, but received request: {req:?}"
                    )
                    .into())
                }
            },
            Some(addr) => addr,
        };

        let bytes: Vec<u8> = request.into();
        let client = reqwest::blocking::Client::new();
        let response = client.post(&format!("http://{addr}")).body(bytes).send()?;
        let response_body = response.bytes()?;
        ParadeWriterResponse::try_from(response_body.to_vec().as_slice()).map_err(|e| e.into())
    }

    fn get_data_directory(name: &str) -> String {
        unsafe {
            let option_name_cstr =
                std::ffi::CString::new("data_directory").expect("failed to create CString");
            let data_dir_str = String::from_utf8(
                std::ffi::CStr::from_ptr(pgrx::pg_sys::GetConfigOptionByName(
                    option_name_cstr.as_ptr(),
                    std::ptr::null_mut(),
                    true,
                ))
                .to_bytes()
                .to_vec(),
            )
            .expect("Failed to convert C string to Rust string");

            format!("{}/{}/{}", data_dir_str, "paradedb", name)
        }
    }

    pub fn insert(&self, index_name: &str, json_builder: JsonBuilder) {
        let response = self
            .send_request(ParadeWriterRequest::Insert(
                Self::get_data_directory(&index_name),
                json_builder,
            ))
            .expect("error while sending insert request}");

        match response {
            ParadeWriterResponse::Ok => {}
            error => panic!("unexpected error while inserting: {error:?}"),
        };
    }

    pub fn shutdown(&self) -> Result<(), Box<dyn Error>> {
        self.send_request(ParadeWriterRequest::Shutdown)?;
        Ok(())
    }
}

unsafe impl PGRXSharedMemory for ParadeWriterClient {}
