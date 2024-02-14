use pgrx::*;

mod init;
mod schema;

static V1_API: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };

#[pg_guard]
#[no_mangle]
extern "C" fn pg_finfo_init() -> &'static pg_sys::Pg_finfo_record {
    &V1_API
}

#[pg_guard]
#[no_mangle]
extern "C" fn pg_finfo_register_s3_schema() -> &'static pg_sys::Pg_finfo_record {
    &V1_API
}
