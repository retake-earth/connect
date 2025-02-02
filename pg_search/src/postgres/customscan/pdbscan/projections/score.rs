// Copyright (c) 2023-2025 Retake, Inc.
//
// This file is part of ParadeDB - Postgres for Search and Analytics
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use crate::nodecast;
use pgrx::pg_sys::expression_tree_walker;
use pgrx::{
    direct_function_call, extension_sql, pg_extern, pg_guard, pg_sys, AnyElement, IntoDatum, PgList,
};
use std::ptr::addr_of_mut;

#[pg_extern(name = "score", stable, parallel_safe, cost = 1)]
fn score_from_relation(_relation_reference: AnyElement) -> f32 {
    panic!("`paradedb.score()` function called in unsupported context.");
}

extension_sql!(
    r#"
ALTER FUNCTION score SUPPORT placeholder_support;
"#,
    name = "score_placeholder",
    requires = [score_from_relation, placeholder_support]
);

pub fn score_funcoid() -> pg_sys::Oid {
    unsafe {
        direct_function_call::<pg_sys::Oid>(
            pg_sys::regprocedurein,
            &[c"paradedb.score(anyelement)".into_datum()],
        )
        .expect("the `paradedb.score(anyelement)` function should exist")
    }
}

pub unsafe fn uses_scores(
    node: *mut pg_sys::Node,
    score_funcoid: pg_sys::Oid,
    rti: pg_sys::Index,
) -> bool {
    #[pg_guard]
    unsafe extern "C" fn walker(node: *mut pg_sys::Node, data: *mut core::ffi::c_void) -> bool {
        if node.is_null() {
            return false;
        }

        if let Some(funcexpr) = nodecast!(FuncExpr, T_FuncExpr, node) {
            let data = data.cast::<Data>();
            if (*funcexpr).funcid == (*data).score_funcoid {
                let args = PgList::<pg_sys::Node>::from_pg((*funcexpr).args);
                assert!(args.len() == 1, "score function must have 1 argument");
                if let Some(var) = nodecast!(Var, T_Var, args.get_ptr(0).unwrap()) {
                    if (*var).varno as i32 == (*data).rti as i32 {
                        return true;
                    }
                }
            }
        }

        expression_tree_walker(node, Some(walker), data)
    }

    struct Data {
        score_funcoid: pg_sys::Oid,
        rti: pg_sys::Index,
    }

    let mut data = Data { score_funcoid, rti };

    walker(node, addr_of_mut!(data).cast())
}

pub unsafe fn is_score_func(node: *mut pg_sys::Node, rti: pg_sys::Index) -> bool {
    if let Some(funcexpr) = nodecast!(FuncExpr, T_FuncExpr, node) {
        if (*funcexpr).funcid == score_funcoid() {
            let args = PgList::<pg_sys::Node>::from_pg((*funcexpr).args);
            assert!(args.len() == 1, "score function must have 1 argument");
            if let Some(var) = nodecast!(Var, T_Var, args.get_ptr(0).unwrap()) {
                if (*var).varno as i32 == rti as i32 {
                    return true;
                }
            }
        }
    }

    false
}
