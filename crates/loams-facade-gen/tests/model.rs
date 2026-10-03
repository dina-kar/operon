//! The model the facade generator builds, exercised against hand-written
//! descriptors (SDK1 Task 3's tests).
//!
//! `golden.rs` runs the same assertions over the real `proto/` tree through
//! `protoc`, so these tests pin the reading of the two extension fields
//! (`loams.options.v1` at 50001/50002) and that one pins the output.

mod support;

use loams_facade_gen::{
    Idempotency, Model, PackageMap, Retry, Streaming, model_from_request, typescript,
};
use support::{Facade, Method, Module, Service};

fn build(services: &[Service]) -> Model {
    let request = support::request("loams.test.v1", services);
    model_from_request(&request, Vec::new()).expect("the descriptor parses")
}

/// Design §44 §7.3, D606: one module per `ModuleOptions` on a service, and one
/// call per `FacadeOptions` on its methods. A service with no `module` option
/// is not in the SDK at all, and a method with no `facade` option is not
/// exposed: annotating is how a service admits an RPC.
#[test]
fn generates_module_per_service_option() {
    let model = build(&[
        Service {
            name: "CollectionService",
            module: Some(Module {
                name: "collections",
                summary: "Namespaces and their collections.",
                unstable: false,
            }),
            methods: vec![
                Method {
                    name: "CreateCollection",
                    input: "loams.test.v1.CreateCollectionRequest",
                    output: "loams.test.v1.CreateCollectionResponse",
                    server_streaming: false,
                    idempotency: 0,
                    facade: vec![support::read_call("createCollection")],
                },
                // No facade option: an admin-only RPC stays out of the SDK.
                Method {
                    name: "DropCollection",
                    input: "loams.test.v1.DropCollectionRequest",
                    output: "loams.test.v1.DropCollectionResponse",
                    server_streaming: false,
                    idempotency: 0,
                    facade: Vec::new(),
                },
            ],
        },
        Service {
            name: "InternalService",
            module: None,
            methods: vec![Method {
                name: "Forward",
                input: "loams.test.v1.ForwardRequest",
                output: "loams.test.v1.ForwardResponse",
                server_streaming: false,
                idempotency: 1,
                facade: vec![support::read_call("forward")],
            }],
        },
    ]);

    assert_eq!(
        model.modules.len(),
        1,
        "only an annotated service is a module"
    );
    let module = model.module("collections").expect("the module");
    assert_eq!(module.summary, "Namespaces and their collections.");
    assert_eq!(module.service, "loams.test.v1.CollectionService");
    assert!(!module.unstable);
    assert_eq!(
        module
            .calls
            .iter()
            .map(|call| call.name.as_str())
            .collect::<Vec<_>>(),
        vec!["createCollection"],
        "an unannotated method is not exposed"
    );
    assert_eq!(
        module.calls[0].full_name(),
        "loams.collections.createCollection",
        "a call is identified the way a caller writes it"
    );
    assert_eq!(
        module.calls[0].rpc(),
        "loams.test.v1.CollectionService/CreateCollection",
        "and by the RPC that backs it"
    );
}

/// Design §44 §7.3: `QueryService/Search` backs two facade calls of one RPC
/// (`loams.vector.search` and `loams.search.query`), which is why
/// `FacadeOptions` is repeated. Both bindings carry the same RPC and the same
/// request and response types, and each is its own method on its own module.
#[test]
fn vector_and_search_share_one_rpc() {
    let model = build(&[Service {
        name: "QueryService",
        module: Some(Module {
            name: "search",
            summary: "Text, hybrid and graph search.",
            unstable: false,
        }),
        methods: vec![Method {
            name: "Search",
            input: "loams.test.v1.SearchRequest",
            output: "loams.test.v1.SearchResponse",
            server_streaming: false,
            idempotency: 1,
            facade: vec![
                Facade {
                    module: "vector",
                    name: "search",
                    retry_safe: None,
                    pagination: None,
                },
                Facade {
                    module: "search",
                    name: "query",
                    retry_safe: None,
                    pagination: None,
                },
            ],
        }],
    }]);

    let search = model.module("search").expect("loams.search");
    let vector = model.module("vector").expect("loams.vector");
    assert_eq!(search.calls.len(), 1);
    assert_eq!(vector.calls.len(), 1);
    assert_eq!(search.calls[0].method, vector.calls[0].method);
    assert_eq!(search.calls[0].method, "Search");
    assert_eq!(search.calls[0].input, vector.calls[0].input);
    assert_eq!(search.calls[0].output, vector.calls[0].output);
    assert_eq!(vector.calls[0].module, "vector");
}

/// Design §44 §7.4, D610: a read and an idempotent RPC retry on their own; a
/// mutation does not, unless `FacadeOptions.retry_safe` says it may. An unset
/// `retry_safe` is not `false`: it means "derive it from the idempotency
/// level".
#[test]
fn retry_class_follows_idempotency_level() {
    let model = build(&[Service {
        name: "ThingService",
        module: Some(Module {
            name: "things",
            summary: "Things.",
            unstable: false,
        }),
        methods: vec![
            Method {
                name: "Get",
                input: "loams.test.v1.GetRequest",
                output: "loams.test.v1.GetResponse",
                server_streaming: false,
                idempotency: 1,
                facade: vec![support::read_call("get")],
            },
            Method {
                name: "Put",
                input: "loams.test.v1.PutRequest",
                output: "loams.test.v1.PutResponse",
                server_streaming: false,
                idempotency: 2,
                facade: vec![support::read_call("put")],
            },
            Method {
                name: "Write",
                input: "loams.test.v1.WriteRequest",
                output: "loams.test.v1.WriteResponse",
                server_streaming: false,
                idempotency: 0,
                facade: vec![support::read_call("write")],
            },
            Method {
                name: "Delete",
                input: "loams.test.v1.DeleteRequest",
                output: "loams.test.v1.DeleteResponse",
                server_streaming: false,
                idempotency: 0,
                facade: vec![Facade {
                    module: "",
                    name: "delete",
                    retry_safe: Some(true),
                    pagination: None,
                }],
            },
            Method {
                name: "Append",
                input: "loams.test.v1.AppendRequest",
                output: "loams.test.v1.AppendResponse",
                server_streaming: false,
                idempotency: 2,
                facade: vec![Facade {
                    module: "",
                    name: "append",
                    retry_safe: Some(false),
                    pagination: None,
                }],
            },
        ],
    }]);

    let module = model.module("things").expect("the module");
    let retry = |name: &str| {
        let call = module.call(name).unwrap_or_else(|| panic!("{name}"));
        (call.idempotency, call.retry)
    };
    assert_eq!(retry("get"), (Idempotency::NoSideEffects, Retry::Safe));
    assert_eq!(retry("put"), (Idempotency::Idempotent, Retry::Safe));
    assert_eq!(retry("write"), (Idempotency::None, Retry::Manual));
    // The two overrides win in both directions.
    assert_eq!(retry("delete").1, Retry::Safe);
    assert_eq!(retry("append").1, Retry::Manual);
}

/// Design §44 §7.4: AIP-158 pagination is named by `FacadeOptions.pagination`
/// as `"<items field>:<next page token field>"`, and the facade turns it into
/// an iterator that follows the tokens. A malformed value fails generation
/// rather than producing an iterator that silently yields one page.
#[test]
fn pagination_iterator_for_list_rpcs() {
    let model = build(&[Service {
        name: "CollectionService",
        module: Some(Module {
            name: "collections",
            summary: "Collections.",
            unstable: false,
        }),
        methods: vec![
            Method {
                name: "ListCollections",
                input: "loams.test.v1.ListCollectionsRequest",
                output: "loams.test.v1.ListCollectionsResponse",
                server_streaming: false,
                idempotency: 1,
                facade: vec![Facade {
                    module: "",
                    name: "listCollections",
                    retry_safe: None,
                    pagination: Some("collections:next_page_token"),
                }],
            },
            Method {
                name: "Watch",
                input: "loams.test.v1.WatchRequest",
                output: "loams.test.v1.Transition",
                server_streaming: true,
                idempotency: 1,
                facade: vec![support::read_call("watch")],
            },
        ],
    }]);

    let module = model.module("collections").expect("the module");
    let paging = module.call("listCollections").expect("the call");
    let pagination = paging.pagination.as_ref().expect("paged");
    assert_eq!(pagination.items, "collections");
    assert_eq!(pagination.next_page_token, "next_page_token");
    assert_eq!(
        module.call("watch").expect("watch").streaming,
        Streaming::Server
    );
    assert!(module.call("watch").expect("watch").pagination.is_none());

    let broken = support::request(
        "loams.test.v1",
        &[Service {
            name: "CollectionService",
            module: Some(Module {
                name: "collections",
                summary: "Collections.",
                unstable: false,
            }),
            methods: vec![Method {
                name: "ListCollections",
                input: "loams.test.v1.ListCollectionsRequest",
                output: "loams.test.v1.ListCollectionsResponse",
                server_streaming: false,
                idempotency: 1,
                facade: vec![Facade {
                    module: "",
                    name: "listCollections",
                    retry_safe: None,
                    pagination: Some("collections"),
                }],
            }],
        }],
    );
    let err = model_from_request(&broken, Vec::new())
        .expect_err("a malformed pagination value fails generation");
    assert!(format!("{err:#}").contains("pagination"), "{err:#}");
}

/// `unstable` reaches the generated SDK, so a module whose wire contract may
/// still change is marked experimental rather than trusted (§44 §10.3).
#[test]
fn an_unstable_package_is_marked() {
    let model = build(&[Service {
        name: "LiveService",
        module: Some(Module {
            name: "live",
            summary: "Live sync.",
            unstable: true,
        }),
        methods: vec![Method {
            name: "Watch",
            input: "loams.test.v1.WatchRequest",
            output: "loams.test.v1.Transition",
            server_streaming: true,
            idempotency: 0,
            facade: vec![support::read_call("watch")],
        }],
    }]);
    assert!(model.module("live").expect("live").unstable);
}

/// The renderer names a package the SDK cannot import as an error rather than
/// emitting TypeScript that will not resolve: the package map is what tells it
/// where `@loams/proto` keeps each proto package.
#[test]
fn a_package_outside_the_map_fails_generation() {
    let model = build(&[Service {
        name: "ThingService",
        module: Some(Module {
            name: "things",
            summary: "Things.",
            unstable: false,
        }),
        methods: vec![Method {
            name: "Get",
            input: "loams.test.v1.GetRequest",
            output: "loams.test.v1.GetResponse",
            server_streaming: false,
            idempotency: 1,
            facade: vec![support::read_call("get")],
        }],
    }]);
    let packages = PackageMap::parse(&["loams.instance.v1=@loams/proto/instance"]).expect("map");
    packages
        .get("loams.instance.v1")
        .expect("the mapped package");
    let err = typescript::render(&model, &packages, "v1").expect_err("unmapped package");
    assert!(err.contains("loams.test.v1"), "{err}");
}
