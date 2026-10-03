# 40 — Loams SystemOne: One Decision API, a Backend per Host

Status: **Proposed**, 2026-10-02. Decisions **D520–D539**, open questions **Q520–Q539** (recorded in the [decision log](13-decision-log.md)). Plans: [SO1](../plans/2026-10-02-so1-systemone.md) (engine, API, backends, CLI) and [SO2](../plans/2026-10-02-so2-systemone-desktop.md) (desktop, browser, Apple sidecar, Loams Bot).

The owner's request of 2026-10-02: "add this to our desktop app", with a pasted note that proposes one `POST /v1/systemone` whose backend is chosen from the host, with Jev (TypeSafe's closed cloud API) as a configurable cloud option. **The note is a third party's reading of third-party pages.** This document re-checked its important claims on 2026-10-02 (§2) and corrects the ones that were wrong. Nothing named `loam-systemone.ts` exists in any of our repositories, so this is a fresh design. **No code is written by this document.**

## 1. Summary

**SystemOne** is a class of small encoder models that answer a typed question about a piece of text in one forward pass, with no generated tokens: `choice` (probabilities over named options), `score` (probabilities over ordered rubric levels and an expected level) and `noul` (P(yes)). Laya (Convai Innovations, Apache-2.0 code and weights) is the open one; Jev (TypeSafe) is the closed cloud one. They answer in 3 to 40 ms locally against about 240 ms for a cloud call, which is what makes them useful as a **decision primitive**: triage, routing, urgency, guardrails, "should I call the big model at all". §39's A2A agents need exactly that (Loams Bot decides which platform agent gets a message, how urgent an alert is, whether a reply needs a person).

What we build (and what we buy):

| Piece | Choice | Buy or build |
|---|---|---|
| The model | Laya `english`, `multilingual` and `typed-decisions` checkpoints (Apache-2.0), pinned by revision and SHA-256 | Buy |
| Wire contract | `loams.systemone.v1` (Connect) and `POST /v1/systemone`, a **superset of the Jev/Laya JSON** so `laya-client`, TypeSafe clients and `laya-serve` interoperate by changing a base URL | Build (small) |
| Engine | A Rust crate `loams-systemone`: canonical types, a `DecisionBackend` trait, a registry, host detection, routing, calibration, self-test, model manager | Build |
| NVIDIA and generic CPU | Upstream `laya-serve` (`pip install "laya[serve]"`) as a **loopback sidecar with a generated key** | Buy |
| Apple silicon | A small Swift sidecar over FluidInference's `FluidUse` library (`LayaManager`, Core ML, Apache-2.0) | Buy plus a thin wrapper |
| CPU without Python | In-process Rust. **Spike-gated** (SO1 Task 0): `kevala` (Apache-2.0, Rust, runs Laya natively) first; `ort` with our own port of Laya's prompt and head only if kevala fails | Buy first |
| Browser | Official `laya-ts` (Apache-2.0, ONNX on onnxruntime-web, WebGPU then WASM) as an **opt-in** cordis plugin; the default browser path is the server | Buy |
| Cloud | A Jev adapter, off by default, key supplied by the user, never a default fallback for private data | Build (adapter only) |

Backend per host, in one table (rules in §6):

| Host | Default backend | Notes |
|---|---|---|
| Loams Desktop on Apple silicon (macOS 14+) | `laya-coreml` (multilingual) | 128/256/512/1024-token buckets, 32 options per question |
| Desktop or CLI with an NVIDIA GPU | `laya-torch` through `laya-serve` on CUDA | 32.8 to 39.5 ms per question on a T4 (vendor figure) |
| Desktop or CLI, CPU only | `laya-native` (kevala) if SO1 Task 0 passes, else `laya-torch` on CPU | Upstream measures 193 to 464 ms per question for torch on CPU; the native number is to be measured |
| Loams server (gateway role) | NVIDIA if present, else CPU, as above | The same sidecar manager runs server-side |
| Browser console | The instance's `/v1/systemone` | Local WASM is an opt-in plugin, and COOP/COEP makes it awkward (§7.5) |
| Any host, explicit opt-in | `jev` | Data leaves the machine; TypeSafe's agreement forbids a standalone service and distillation; OSS-client fit unconfirmed (Q520) |

## 2. What was verified, and what the note got wrong (2026-10-02)

Every row below was checked on 2026-10-02 against the named page or API. "Vendor figure" means the number is the model author's, not ours; SO1 Task 0 re-measures what we depend on.

### 2.1 Licences

| Item | Finding | Source |
|---|---|---|
| Laya **model** | **Apache-2.0** ("Apache 2.0 · Convai Innovations"; Hugging Face model card `license: apache-2.0`, tag `commercial-use`). Three checkpoints in one repo: English root (ModernBERT-large, 421M, 512-token context), `multilingual/` (mmBERT-base, 322M, 1024 tokens, up to 8192 with `max_len`) and `typed-decisions/` (421M, 1024) | huggingface.co/convaiinnovations/laya |
| Laya **code** | **Apache-2.0** (`LICENSE` in `NandhaKishorM/laya`, the official repository, about 30 k stars; PyPI `laya` 0.3.23 says Apache-2.0). The note's "convaiinnovations/laya" is the **Hugging Face** repo; there is no GitHub repo of that name | github.com/NandhaKishorM/laya, pypi.org/project/laya |
| `FluidInference/laya-coreml` | **Apache-2.0**, "matching the original weights", conversion "independent and unofficial". `LICENSE` and `NOTICE.md` are in the repo. Multilingual only; 128/256/512/1024-token buckets, **32 option slots** each; fp16 about 614 MB per bucket, int8-embedding variants smaller | huggingface.co/FluidInference/laya-coreml |
| `FluidInference/FluidUse` (`LayaManager`) | **Apache-2.0**; a Swift package (tools 6.0, **macOS 14**) that exports `.library(name: "FluidUse")` and depends on `FluidAudio`. 3.7 ms per short question on an M5 Pro (README) | github.com/FluidInference/FluidUse |
| `aac6fef/laya-mlx` | **Not a GitHub repository.** `aac6fef` is a Hugging Face user; `aac6fef/laya-mlx` is the **weights** repo (Apache-2.0 on Hugging Face). The code is **`mizorewww/laya-mlx`** (Apache-2.0, "an independent MLX port, not an official Convai Innovations release"). **It is not English only**: it supports the English, multilingual and typed-decisions checkpoints (13.4 ms English, 7.4 ms multilingual, M3 Max, vendor figure). Python 3.11+, macOS 14+, no server | github.com/mizorewww/laya-mlx, huggingface.co/aac6fef/laya-mlx |
| `nvkudva/laya-web` | **No licence.** The repository has no `LICENSE`, and GitHub reports none, so by default all rights are reserved: **we do not copy or depend on it.** Facts it states: 524 MB int8 weights (1.69 GB fp32), onnxruntime-web, COOP `same-origin` and COEP `require-corp` for threaded WASM, main thread in production, about 340 ms on short states and 2.4 s at 512 tokens, single-thread fallback "roughly 6x slower" | github.com/nvkudva/laya-web |
| Better browser sources | Official **`laya-ts`** (inside `NandhaKishorM/laya`, Apache-2.0): split `encoder.onnx` + `head.onnx`, Node and browser, "WebGPU to WASM fallback", `onnxruntime-web` an optional peer dependency. **`bvolpato/kevala`** (Apache-2.0): Rust with zero dependencies compiled to WASM plus WebGPU kernels and a **native CPU CLI**; Laya int8 pack 479 MB; "works from any origin and needs no special headers". **`vishalmysore/layaForWeb`** (Apache-2.0) | github.com/NandhaKishorM/laya (`laya-ts/`), github.com/bvolpato/kevala |
| `browser-use/jev-ultrafast` | **MIT.** It is a browser-agent sample that **calls** the Jev API. Its licence says nothing about the API's terms | github.com/browser-use/jev-ultrafast |
| **Jev API terms** | TypeSafe publishes Terms of Use (website only), a Privacy Policy, an Acceptable Use Policy and a **Master Customer Agreement** (published 2026-09-28) at typesafe.ai/legal. The MCA covers the API: the licence "includes the right to include the API into one or more software applications developed and operated by Customer"; the customer "will not ... offer or make the Services available as a standalone service", and "will not use the Services or any Output to perform model distillation, train a model to imitate the output of the Services, or develop a similar or competing product". TypeSafe will not put Customer Data in a training dataset without prior consent. `docs.typesafe.ai/api` publishes limits of **255 options per `choice` and 2 to 10 levels per `score`**, accepts string, object or array `instructions` and `criteria`, makes `noul` `criteria` optional, and answers 401, 422, 429 and 529; numeric rate limits and a token budget are not published; the model is closed. **Consequences for us:** each user brings their own agreement and key; a Loams-hosted pass-through looks like a "standalone service" and is not planned; **Jev outputs must never be used as training labels** for Laya or any Loams model (§9.4). Whether an OSS client with a user-supplied key and a selectable-backend listing fit is for TypeSafe to confirm: **owner action** (Q520). This corrects an earlier reading of this row that found no API terms | typesafe.ai/legal/mca, /terms, /privacy-policy, docs.typesafe.ai |

### 2.2 The API surface

| Claim in the note | Finding |
|---|---|
| `laya-serve` response fields for `noul` and `score` are undocumented | **Documented** (`docs/http-api.md` in the Laya repo). `choice`: `choice`, `probabilities`. `score`: `score` (expected level index, may fall between levels), `probabilities` keyed `"0".."k-1"`, `legend`. `noul`: `noul`, the probability of the yes option. All: `confidence`, `answer_confidence`, `action.act_probability`. Plus `usage` (`input_tokens`, `output_tokens: 0`, `state_tokens`, `state_tokens_dropped`, `truncated`, `truncated_questions`, `options`) and `routing` (`model`, `repo`, `reason`, `detection`, `workflow`) |
| (not in the note) `confidence` versus `answer_confidence` | **They differ and must not share a threshold.** `answer_confidence` is `max(p)`, the quantity temperature scaling fits; `confidence` is normalised entropy `1 - H(p)/log(k)` on `choice` and `score` and `max(p_yes, p_no)` on `noul`. Jev defines confidence as `(n*p_max - 1)/(n - 1)` (as stated by the Laya docs; not independently checked), so a threshold carried over from Jev gates differently on Laya |
| (not in the note) request controls | `model`, `task`, `lang`, `lang_guess`, `max_len`, `head_max_len`, `min_confidence` (an answer below it comes back `low_confidence: true`, the answer is kept); `POST /v1/systemone/batch` for one question set over many states; `GET /health` |
| (not in the note) server limits | Body 2 MiB; `state` 50 000 characters; 64 questions per request; 100 options per `choice`; 32 levels per `score`; 512 options per request; 16 concurrent requests (`LAYA_MAX_CONCURRENT`, excess gets 503). Errors 400/401/413/422/500/503. One forward pass at a time (a single-worker executor) |
| `laya-serve` binds 0.0.0.0 with no auth unless `LAYA_API_KEY` is set | **Confirmed**, along with `LAYA_HOST` (default `0.0.0.0`), `LAYA_PORT` (8000), `LAYA_DEVICE`, `LAYA_PRELOAD`, `LAYA_MODELS`, `LAYA_THREADS`, `LAYA_DEFAULT_MODEL`, `LAYA_ROOT_PATH`. `/health` with a key set answers only `{"status":"ok"}` to callers without it. Also `LAYA_REVISION` and per-file digests for checkpoint integrity (`docs/security.md`) |
| Latency 32.8 to 39.5 ms on a T4 | **Confirmed as the vendor's figure** (English 39.5 ms, multilingual 32.8 ms, one question; 10 questions batched 158.6 and 72.3 ms). Jev p50 236 to 276 ms is cited from two third-party benchmarks by the Laya README, not measured by Convai |
| Core ML 3.6 to 17.9 ms | 3.6 ms (HF card) or 3.7 ms (FluidUse README) on an M5 Pro for the smallest bucket; 5.2 ms median over 3 899 questions. The 17.9 ms upper end was not found and is not used |
| `laya-onnx` has no published latency or accuracy | **Partly.** `scripts/export_onnx.py` and `ONNXAgent` exist (`laya[onnx]`); upstream says int8 is "roughly 2x faster than eager" on CPU but "trades real accuracy... do not use it where the calibrated probability or confidence matters" (per-tensor int8 on the English model agreed with eager on 64/96 decisions). fp32 ONNX is the safe export. No published latency for fp32 ONNX |
| Banking77: Laya 0.425, Jev 0.870 | **Confirmed**, with the correction that Jev's score was on **72** labels and Laya's on **77**; the cause is the fixed option-token budget (`head_max_len` 192 English, 256 multilingual, so 77 options get 3 to 4 tokens each). **Upstream's own fix is to raise `head_max_len` and `max_len`** (for example 512 and 1024) or to split into a coarse-to-fine two-step choice. Those are request controls on `laya-serve`; Core ML's fixed 32-option bucket cannot widen |
| Zero-shot quality is modest | **Confirmed, and worse than modest**: on the typed-decisions benchmark the base checkpoints score 0.362 (English) and 0.342 (multilingual), **below the 0.461 majority-class baseline**; the fine-tuned `laya-typed-decisions` scores 0.766 against Jev's published 0.727. Upstream says "Laya is a fast base to specialise, not a zero-shot decision engine" |
| `score` is weakest | Confirmed (SST-5 0.372) |
| Over-confident, calibrate per (type, option count) | Confirmed: refitting one temperature per (question type, option count) moves mean ECE 0.466 to 0.081 (English) and 0.314 to 0.106 (multilingual) |
| Do not use `act_probability` | Confirmed: it reads 1.0 for almost every input (AUROC 0.30 on 396 labelled decisions, upstream issue 185) |
| `noul` workaround A/B | Confirmed: `noul` renders its options as `false:` / `true:` and "that label pair can dominate the answer, returning a confident 'no' for clearly positive input" (upstream issue 156), most strongly on the English checkpoint. Upstream's workaround is a two-option `choice` with neutral keys `A`/`B` |
| **For Jev, map `noul` and `score` onto choices** | **Not needed.** Jev documents all three primitives natively (`docs.typesafe.ai`): `noul` returns a probability in a `noul` field with no confidence field; `score` returns `score`, `legend`, `probabilities`, `confidence`; `choice` returns `choice`, `probabilities`, `confidence`. The same shape as Laya's. Strong with more than 20 options is third-party only, and Jev's published cap is 255 options (docs.typesafe.ai/api, 2026-10-02) |
| English checkpoint gives confident wrong answers | Confirmed and sharper: Khmer scores 0.000 accuracy at 0.952 confidence, so confidence gating cannot save you; only 23 of 51 languages are usable on the English checkpoint against 45 of 51 on the multilingual one |
| A state over the context window | `laya-serve` truncates and **reports it** in `usage.truncated`, `state_tokens_dropped` and `truncated_questions` (the cut is silent otherwise). Core ML's `LayaManager` picks the smallest loaded bucket that fits and truncates on the right in the largest |

### 2.3 Open question 1 of the note: "cordis engine"

The note asked whether "cordis engine" meant Core ML. **It does not.** **cordis is the JavaScript plugin framework the Loams console is built on (§37 D422): contexts, services and a plugin lifecycle with scoped disposal and hot reload. It is not an inference engine.** Core ML is Apple's on-device inference framework and is a separate thing. In this design cordis appears only as the host of the optional browser plugin (§7.5), and Core ML appears only in the macOS sidecar (§7.3).

### 2.4 Prefer an official Rust path: what is realistic

| Option | Status on 2026-10-02 | Verdict |
|---|---|---|
| `ort` (ONNX Runtime bindings for Rust) | Latest on crates.io is **2.0.0-rc.13** (2026-07-28): still a release candidate. Laya's export is a split `encoder.onnx` + `head.onnx` plus tokenizer; the Python code builds the prompt (byte-exact `json.dumps` of the state, option markers, token budgets, language routing, calibration). Using `ort` means **porting that Python** and keeping it in step with upstream | Possible, **expensive and brittle**: the work is not the runtime, it is template and head parity. Only if `kevala` fails |
| `candle` | `candle-transformers` has a ModernBERT model, but Laya adds a 2-layer decision head with an option-marker scorer and an act head, and mmBERT for the multilingual checkpoint | Same porting cost as `ort`, with no export step. Not first choice |
| **`kevala`** (`bvolpato/kevala`, Apache-2.0, v0.1.3) | A Rust workspace (`kevala`, `kevala-cli`, `kevala-wasm`), zero dependencies, that already implements Laya's JSON serialisation, byte-level BPE tokenizer, request template, ModernBERT-large engine, decision head and response, int8 `.kevala` packs (Laya 479 MB), a native 64-bit CPU CLI and WASM/WebGPU builds. **Not on crates.io** (`kevala` does not exist there), 9 stars, one author, last pushed 2026-09-29 | The only realistic official-style Rust path: depend on it as a **git dependency pinned to a commit**, behind a trait, with a parity test against `laya-serve`. Risk: one maintainer, young, English-checkpoint packs only seen (the multilingual pack is not in its table) |
| Python sidecar (`laya-serve`) | Official, documented, current (0.3.23), everything works including `/health`, batch, `min_confidence`, language routing | **The v1 default off-Apple**. Cost: a Python 3.10+ environment with PyTorch (GBs); acceptable on servers and NVIDIA hosts, unwelcome on a desktop |

## 3. Goals, non-goals

**Goals.** One decision API for the engine, CLI, desktop, browser and agents; the best local backend per host chosen automatically and overridable; no data leaves the machine unless the owner of the data said so; calibrated, comparable confidences across backends; verified, pinned, resumable model downloads; a clean seam for fine-tuned checkpoints; the desktop integration the owner asked for.

**Non-goals.** Text generation; distilling or imitating Jev (forbidden by its agreement); training inside the engine (fine-tuning stays upstream's notebooks, §9.4); a Loams-hosted multi-tenant inference API (that is `loam-platform`, §11); GPU kernels of our own; replacing LLMs (SystemOne decides, it does not write); a decision that approves anything (D537).

## 4. The API (D520, D521, D522)

### 4.1 `loams.systemone.v1` (Connect)

File `proto/loams/systemone/v1/systemone.proto`. The messages mirror the Jev/Laya JSON, but **the HTTP form of §4.2 is a separate, hand-specified JSON dialect, not ProtoJSON**: Connect clients that use JSON get standard ProtoJSON (lowerCamelCase names, enum names such as `ALLOW_CLOUD`, `budget` as a nested object), while `POST /v1/systemone` uses the Jev/Laya names (snake_case, lowercase enum strings, `max_len` and `head_max_len` at the top level). `wire.rs` translates between the two, once, and SO1 Task 2 tests the mapping in both directions.

```proto
syntax = "proto3";
package loams.systemone.v1;

import "google/protobuf/struct.proto";

service SystemOneService {
  rpc Decide(DecideRequest) returns (DecideResponse);
  rpc DecideBatch(DecideBatchRequest) returns (DecideBatchResponse);   // one question set, many states
  rpc ListBackends(ListBackendsRequest) returns (ListBackendsResponse);
  rpc SelfTest(SelfTestRequest) returns (SelfTestResponse);
}

message DecideRequest {
  google.protobuf.Value state = 1;               // text or JSON; required
  map<string, Question> questions = 2;           // 1..64
  string backend = 3;                            // optional per-request override (D524)
  string lang = 4;                               // optional BCP 47 hint ("de", "en-US")
  optional double min_confidence = 5;            // abstain below this calibrated confidence (D536)
  Budget budget = 6;                             // optional max_len / head_max_len
  Locality locality = 7;                         // LOCALITY_UNSPECIFIED is treated as LOCAL_ONLY everywhere (D525)
  string idempotency_key = 8;                    // optional; for logs only, decisions are pure
}

message Question {
  QuestionType type = 1;                         // CHOICE, SCORE, NOUL
  string instructions = 2;
  oneof criteria {                               // required for CHOICE and SCORE
    ChoiceCriteria choice = 3;                   // ordered map option -> description
    ScoreCriteria score = 4;                     // ordered list of level texts
  }
}
message ChoiceCriteria { repeated ChoiceOption options = 1; }          // order is preserved
message ChoiceOption   { string key = 1; string description = 2; }
message ScoreCriteria  { repeated string levels = 1; }
enum QuestionType { QUESTION_TYPE_UNSPECIFIED = 0; CHOICE = 1; SCORE = 2; NOUL = 3; }
enum Locality     { LOCALITY_UNSPECIFIED = 0; LOCAL_ONLY = 1; ALLOW_CLOUD = 2; }
message Budget    { uint32 max_len = 1; uint32 head_max_len = 2; }

message DecideResponse {
  string model = 1;                              // "laya-rl-agent" for Laya, "jev-1.13.0" for Jev
  map<string, Answer> answers = 2;
  Usage usage = 3;
  Provenance loams = 4;                          // what answered, how long it took, what was done to the numbers
}
message Answer {
  QuestionType type = 1;
  oneof value { string choice = 2; double score = 3; double noul = 4; }
  map<string, double> probabilities = 5;         // choice: by key; score: by level index "0".."k-1"
  map<string, string> legend = 6;                // score only
  optional double confidence = 7;                // backend-native, never compared across backends
  optional double answer_confidence = 8;         // max(p) as the backend reports it
  double calibrated_confidence = 9;              // Loams: uniform meaning on every backend (D536)
  bool low_confidence = 10;                      // calibrated_confidence < min_confidence
}
message OptionSpans { uint32 total = 1; uint32 distinct = 2; double tokens_per_option = 3; }
message Usage {
  uint32 input_tokens = 1; uint32 output_tokens = 2; uint32 state_tokens = 3;
  uint32 state_tokens_dropped = 4; bool truncated = 5; repeated string truncated_questions = 6;
  map<string, OptionSpans> options = 7;          // Laya: present only when a question's options lost their token spans
}
message Provenance {
  string backend = 1;                            // laya-coreml | laya-torch | laya-native | laya-mlx | laya-wasm | jev
  string checkpoint = 2;                         // english | multilingual | typed-decisions | <custom id>
  string revision = 3;                           // pinned model revision or digest
  string device = 4;                             // "ane", "cuda:0", "cpu", "cloud"
  Locality locality = 5;                         // where the state went
  double latency_ms = 6;
  string calibration = 7;                        // calibration table id, or "" if raw
  repeated string warnings = 8;                  // "truncated", "options_over_20", "fell_back_to:jev", ...
  string route_reason = 9;                       // one sentence
}
```

`DecideBatch` takes `states[]` and one `questions` set and returns `responses[]` in order (Laya's `/v1/systemone/batch`). `ListBackends` returns each registered backend with `id`, `locality`, `status`, `capabilities` and the reason it is not ready. `SelfTest` runs the fixed set (§9.2) and returns per-case results.

**Errors** use `loams.errors.v1` with these reasons: `INVALID_QUESTION` (422: unknown type, missing criteria, options over the backend's cap), `STATE_TOO_LARGE` (413), `NO_BACKEND` (the router found none that is ready and allowed), `CLOUD_NOT_ALLOWED` (a request asked for `jev` but locality or policy forbids it), `BACKEND_UNAVAILABLE` (503, with `retry_after`), `MODEL_NOT_INSTALLED` (with the `loams systemone model pull` command), `SELF_TEST_FAILED`.

### 4.2 HTTP `POST /v1/systemone` (D521)

The path, request and response follow **Jev's documented fields** (and `laya-serve`'s, which follows Jev's), so a client that uses only those works by changing its base URL. Loams adds optional fields and never renames or removes a Jev-defined one. Where this surface differs from `laya-serve`, it is listed here, not promised away:

| Laya feature | Here |
|---|---|
| Request `task`, `lang_guess` | Accepted; forwarded to Laya backends, ignored by others |
| Request hook arguments (`hooks`, `on_predict_start`, ...) | `422`, as Laya does |
| Response `routing`, `action` | Not returned (our `loams` block replaces `routing`; `act_probability` is never exposed) |
| `model` | Accepted and ignored (the router picks); a Jev id is never an error |
| `min_confidence` | Applied to `calibrated_confidence`, not to Laya's `answer_confidence` |
| Response `usage.options` | Returned (Laya's per-question option-span report) |
| `instructions` and `criteria` shapes | **v1 supports string `instructions`** and the `criteria` forms below. Jev also accepts object or array `instructions` and free-form `criteria`; those answer `422 UNSUPPORTED_SHAPE` in v1 (a client sending them is not base-URL compatible), and a later version may serialise them to text once we measure whether that helps |
| `noul` `criteria` (optional in Jev) | Accepted as an optional string; forwarded to Jev; for Laya backends ignored with the warning `noul_criteria_ignored` (Task 0 checks whether Laya reads it) |

```json
POST /v1/systemone
Authorization: Bearer <loams token>        // required except on a loopback desktop listener with a local key
{
  "state": "I was charged twice this month, I want my money back",
  "questions": {
    "queue":   {"type": "choice", "instructions": "Which team?",
                "criteria": {"billing": "billing and refunds", "tech": "login and app issues", "other": "everything else"}},
    "urgency": {"type": "score",  "instructions": "How urgent?", "criteria": ["calm", "firm", "angry", "furious"]},
    "refund":  {"type": "noul",   "instructions": "Does the customer ask for a refund?"}
  },
  "model": "jev-latest",             // accepted for Jev clients; ignored by Loams (the router picks), see "backend"
  "backend": "laya-coreml",          // Loams: optional per-request override
  "lang": "en",                      // Loams (also Laya): optional hint
  "min_confidence": 0.6,             // Loams: calibrated; abstain below it
  "max_len": 1024, "head_max_len": 512,
  "locality": "local_only"           // Loams: local_only (default) | allow_cloud
}
```

`criteria` for `choice` may be a map (key to description), a list of keys, or **the Loams canonical form, a list of `{"key", "description"}` objects**; for `score` a list. JSON objects are unordered by RFC 8259, but Python-based Jev and Laya clients rely on member order, so for the map form we preserve the order of members as received, and we recommend the list forms, whose order is defined. The proto uses the repeated form. Option order feeds the prompt, so it is part of the request's meaning.

```json
200 OK
{
  "model": "laya-rl-agent",
  "answers": {
    "queue":   {"type": "choice", "choice": "billing",
                "probabilities": {"billing": 0.9519, "tech": 0.0327, "other": 0.0154},
                "confidence": 0.797, "answer_confidence": 0.9519,
                "calibrated_confidence": 0.91, "low_confidence": false},
    "urgency": {"type": "score", "score": 1.6994,
                "legend": {"0": "calm", "1": "firm", "2": "angry", "3": "furious"},
                "probabilities": {"0": 0.0249, "1": 0.4136, "2": 0.3985, "3": 0.1629},
                "confidence": 0.1925, "answer_confidence": 0.4136,
                "calibrated_confidence": 0.38, "low_confidence": true},
    "refund":  {"type": "noul", "noul": 0.97, "calibrated_confidence": 0.97, "low_confidence": false}
  },
  "usage": {"input_tokens": 83, "output_tokens": 0, "state_tokens": 12, "state_tokens_dropped": 0,
            "truncated": false, "truncated_questions": []},
  "loams": {"backend": "laya-coreml", "checkpoint": "multilingual", "revision": "<sha>", "device": "ane",
            "locality": "local", "latency_ms": 5.1, "calibration": "cal-2026-10-02-a",
            "warnings": [], "route_reason": "Apple silicon host; 3 options; English text"}
}
```

Rules:
- Unknown request fields are ignored (as Laya does). The five hook arguments Laya refuses are not part of our surface.
- `routing` and `action` are **not** returned (Laya's extensions, not Jev's); `act_probability` is never exposed (D536).
- `noul` answers have no native `confidence` (Jev) or one equal to `answer_confidence` (Laya); `calibrated_confidence` is `max(p, 1-p)` after calibration.
- **Limits, two layers.** Validation (before any backend) uses the **largest limit any registered backend accepts**: body 2 MiB, state 50 000 characters, 64 questions, **255 options per `choice`** (Jev's cap), **32 levels per `score`** (Laya's cap; Jev's is 10), 512 options per request. The router then applies each backend's own capability: Core ML 32 options; `laya-serve` 100 options and 32 levels; Jev 255 options and **2 to 10 levels**. A request over a backend's cap simply excludes that backend; if none remains the error is `NO_BACKEND` with the reason per backend (and a hint to split into a two-step choice). So 32 levels is Loams's and Laya's limit, not a universal Jev-compatible one.
- HTTP status mapping: 400 malformed; 401/403 auth; 413 limits; 422 invalid question; 429 quota; 503 `BACKEND_UNAVAILABLE`; 500 never carries internals.
- **Listener.** On the desktop and in `loams dev` the route is on the loopback listener (D111). On a server it is on the gateway role behind the normal auth (§19) with a per-org quota (the engine enforces whatever limit it is given; the plan decides the number, D220).

### 4.3 How `noul` and `score` are mapped (D522)

Native by default on every backend. Two quirks are handled **inside the adapter**, invisible to callers:
- **Laya `noul` stuck on its labels** (upstream issue 156). Each Laya backend carries a flag `noul_mode: native | choice_ab`. The self-test (§9.2) includes a pair of `noul` cases with opposite correct answers; if a checkpoint answers both the same way with confidence above 0.9, the adapter switches that checkpoint to `choice_ab` (a two-option `choice` with neutral keys `A` yes and `B` no, upstream's workaround) and reports `warnings: ["noul_via_choice_ab"]`. Operators can force either mode in config.
- **Jev needs no mapping.** Its `noul` and `score` are native (§2.2); the note's "map them onto choices for Jev" is dropped. Its `score` is limited to 2 to 10 levels, which the router enforces (§4.2).

**Locality default.** proto3 gives an omitted enum the zero value, `LOCALITY_UNSPECIFIED`. The service, the HTTP adapter and every client library treat it as `LOCAL_ONLY`; only an explicit `ALLOW_CLOUD` can make a cloud backend eligible. A test pins this for both transports (SO1 Task 2).

## 5. The backend registry and trait (D523)

Crate `crates/loams-systemone` (no GPUI, no server code). Backends are values behind one trait; the registry owns their lifecycle.

```rust
// crates/loams-systemone/src/backend.rs
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum BackendId { LayaCoreml, LayaTorch, LayaNative, LayaMlx, LayaWasm, LayaOnnx, Jev }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Locality { Local, Cloud }

pub struct Capabilities {
    pub id: BackendId,
    pub locality: Locality,
    pub question_types: QuestionTypes,            // CHOICE | SCORE | NOUL bits
    pub max_options_per_choice: u32,              // Core ML 32; laya-serve 100; Jev 255
    pub max_levels_per_score: u32,
    pub max_questions: u32,
    pub checkpoints: Vec<Checkpoint>,             // english | multilingual | typed-decisions | custom(id)
    pub languages: LanguageSupport,               // EnglishOnly | Multilingual | ByCheckpoint
    pub context_tokens: ContextWindows,           // per checkpoint; widenable: bool
    pub widenable_budget: bool,                   // true for laya-serve (max_len, head_max_len); false for Core ML
    pub reports_truncation: bool,
    pub batch: bool,
    pub typical_latency_ms: Option<f64>,          // vendor figure, informational only
}

#[derive(Debug)]
pub enum BackendStatus {
    NotInstalled { how: InstallHint },            // model or runtime missing, how to get it
    Installing { progress: f32 },
    Starting,                                     // sidecar launching, model loading
    Ready { device: String, revision: String },
    Degraded { reason: String },                  // answering, self-test failed or fell back to CPU
    Failed { reason: String },
}

#[async_trait::async_trait]
pub trait DecisionBackend: Send + Sync {
    fn capabilities(&self) -> &Capabilities;
    async fn status(&self) -> BackendStatus;
    /// Pure inference on an already validated, already routed request. No policy here.
    async fn decide(&self, req: &BackendRequest, cx: &CallContext) -> Result<RawDecision, BackendError>;
    async fn decide_batch(&self, reqs: &[BackendRequest], cx: &CallContext) -> Vec<Result<RawDecision, BackendError>>;
    async fn start(&self) -> Result<(), BackendError>;      // idempotent; spawns sidecar or loads model
    async fn stop(&self);                                   // releases memory and kills the sidecar
}
```

`BackendRequest` carries the canonical request plus the router's resolved choices (`checkpoint`, `lang`, widened `max_len`/`head_max_len`, `noul_mode`). `RawDecision` is the backend's answer in canonical form (`Answer` without `calibrated_confidence`) plus `Usage`, device and revision; **calibration, abstention, `act_probability` stripping and provenance are applied once, above the trait**, so no adapter can forget them. `BackendError` is `{Invalid, TooLarge, Unavailable{retry_after}, Timeout, Internal}` and is never shown with a path or a stack.

```rust
pub struct Registry { /* BTreeMap<BackendId, Arc<dyn DecisionBackend>>, config, profile */ }
impl Registry {
    pub fn register(&mut self, b: Arc<dyn DecisionBackend>);
    pub async fn list(&self) -> Vec<BackendInfo>;
    pub async fn decide(&self, req: DecideRequest, auth: &Principal) -> Result<DecideResponse, SystemOneError>;
}
pub fn plan(profile: &HostProfile, req: &DecideRequest, cfg: &RoutingConfig,
            avail: &[BackendInfo]) -> Result<RoutePlan, SystemOneError>;   // pure, table-tested (D524, D525)
```

Adapters shipped (SO1 unless noted): `LayaServeBackend` (HTTP client plus supervised sidecar; serves `LayaTorch` on CUDA or CPU), `JevBackend`, `FakeBackend` (tests, deterministic), `LayaNativeBackend` (SO1 Task 0 outcome, kevala), and in SO2 `CoremlSidecarBackend`, `LayaMlxBackend` (optional) and the browser plugin's own `LayaWasmBackend` (TypeScript; it implements the same contract in the cordis plugin, not this trait). `LayaOnnx` is reserved and not planned.

## 6. Host detection and routing (D524, D525)

### 6.1 Host detection

`HostProfile` is computed once per process, on desktop, CLI and server alike, and exposed by `loams systemone status`:

```rust
pub struct HostProfile {
    pub os: Os,                       // MacOs{ major }, Linux, Windows
    pub arch: Arch,                   // Aarch64, X86_64
    pub apple_silicon: bool,          // macOS aarch64
    pub nvidia: Option<NvidiaInfo>,   // from NVML if loadable, else `nvidia-smi -L`; name, vram_mb, driver
    pub cpu_threads: u32, pub ram_mb: u64,
    pub python: Option<PythonInfo>,   // `uv`-managed or system python >= 3.10, for laya-serve
    pub role: Role,                   // Desktop | Cli | Server | Browser (browser: always via server)
    pub in_container: bool,
}
```

Detection never loads a model and never runs inference. A GPU that exists but whose driver cannot load PyTorch is found by the **sidecar's `/health`** (`device` and `cpu_fallbacks`), not guessed by the host probe: `laya-serve` demotes silently to CPU and the health payload says so, and the registry treats `device=cpu` on an NVIDIA host as `Degraded`.

### 6.2 Selection order

1. A **per-request** `backend` beats everything.
2. `LOAMS_SYSTEMONE_BACKEND` (or the config key `systemone.backend`) beats auto-detection.
3. Auto: the table in §1 by `HostProfile`, then readiness (a backend that is `NotInstalled` is skipped with a hint, never downloaded without consent).
4. `LOAMS_SYSTEMONE_FALLBACK` (a backend id, for example `jev`) is tried only on `Unavailable`, `Timeout` or `Internal` from the primary, **and only if locality allows it** (D525). An `Invalid`/`TooLarge` is never retried elsewhere.

### 6.3 Rules (applied by `plan`, in this order; each yields a warning or an exclusion)

| Rule | Behaviour |
|---|---|
| Locality | Default `LOCAL_ONLY`. A cloud backend is eligible only if the request says `ALLOW_CLOUD` **and** the instance, org or user setting allows cloud decisions **and** a key is configured. A forced `backend: "jev"` with locality `LOCAL_ONLY` is an error (`CLOUD_NOT_ALLOWED`), not a quiet fallback |
| Options over a backend's cap | More than 32 options on any `choice`: exclude `laya-coreml` (its bucket has 32 slots). Over 100 excludes `laya-torch` (its HTTP cap), leaving Jev (cap 255) if cloud is allowed; over 255 is rejected at validation. A `score` with more than 10 levels excludes Jev. If no backend remains: `NO_BACKEND` naming each exclusion and suggesting a two-step hierarchy |
| More than about 20 options | Laya's accuracy falls off sharply (0.425 on 77 labels vs Jev's 0.870 on 72). Order: `laya-torch` with a widened `head_max_len`/`max_len` (upstream's fix) first if local-only; Jev preferred only if cloud is allowed (the request's `locality` and the instance switch), otherwise stay local; warning `options_over_20` either way. No quality claim for the widened path until SO1's self-test adds a 40-option case and the numbers are measured (Q528) |
| Language | Only the English checkpoint is English-only. Use it only when the state is detected as English (explicit `lang`, else script and language detection: Latin script and detector says English). **Anything else, including undecided Latin text, goes to the multilingual checkpoint.** A backend that ships only the English checkpoint (`laya-wasm` English, the note's `laya-mlx` English weights) is excluded for non-English text. The cost of the safe default is accuracy on English (MASSIVE intent 0.783 English vs 0.657 multilingual), so on Apple silicon the English-quality gap is Q522 |
| State over the window | Core ML: the smallest loaded bucket that fits; if even 1024 does not fit, `truncated` is reported and the warning `state_truncated` is added. Laya sidecar: `usage.truncated` becomes the same warning, and `on_truncate` set to `warn`, `widen` or `error` (default `warn`; `widen` retries once with a larger `max_len` up to `LAYA_MAX_TOKEN_BUDGET`). The state is never silently cut |
| Question types | All three are native on every backend; `noul_mode` per §4.3 |
| Error | Primary fails with a retryable error: the fallback (if set and allowed) once; the response carries `fell_back_to:<id>` |
| Readiness | A backend whose self-test failed is `Degraded` and is skipped unless it is the only one, in which case the answer carries `warnings: ["self_test_failed"]` |

`RoutePlan` is `{ primary, fallback: Option<BackendId>, request_overrides, warnings, reason }`. The reason string is the `route_reason` the caller sees.

## 7. Sidecars and runtimes, per backend (D526 to D530)

### 7.1 `laya-torch`: `laya-serve` on loopback with a generated key (D526)

| Concern | Decision |
|---|---|
| Install | `loams systemone install laya` creates `$LOAMS_HOME/systemone/venv` with `uv` (pinned `uv`, Python 3.12) and installs `laya[serve]==<pin>` (0.3.23 on 2026-10-02) with a hash-pinned requirements file we generate and review per release. CUDA vs CPU torch wheels chosen from `HostProfile`. Nothing is installed without consent |
| Bind | `LAYA_HOST=127.0.0.1`, `LAYA_PORT=<free port, picked and held>`, never the `0.0.0.0` default. The adapter **refuses to talk to a non-loopback address** (D111, same rule as the Postgres listener) |
| Auth | A random 256-bit key generated per launch, passed as `LAYA_API_KEY` in the child's environment (never on a command line, never logged, never written to disk), held by the supervisor and sent as `Authorization: Bearer`. Without it `/health` returns only `{"status":"ok"}`, so the supervisor reads health with the key |
| Device | `LAYA_DEVICE=cuda` on NVIDIA hosts, else unset (auto). `LAYA_PRELOAD=1`, `LAYA_MODELS=english,multilingual` (typed-decisions only if installed). `LAYA_THREADS` = physical cores, never logical (upstream: oversubscribing "is a large regression"). `LAYA_MAX_CONCURRENT=16` |
| Integrity | `LAYA_REVISION` pinned to a reviewed SHA per checkpoint, with `standalone_repos` semantics handled as upstream documents, and per-file SHA-256 digests from our lock file (§9.5). Offline: `HF_HUB_OFFLINE=1` once cached |
| Supervision | The crate's `SidecarSupervisor`: spawn in its own process group, wait for `/health` (`loaded` lists the checkpoints), backoff 1 s to 30 s, five tries in ten minutes then `Failed`, kill on drop, kill the group on stop. One forward pass at a time is upstream's model, so the adapter holds its own queue of depth 16 and returns `Unavailable{retry_after}` beyond it |
| Why not embed Python (PyO3) | Link size, GIL, crash isolation and a second supervisor for one directory; the same reasons §37 §18.3 gives against embedding the engine |
| Windows | CPU via the same venv; NVIDIA via the CUDA wheel. Not a launch target for the desktop (the app is remote-only on Windows per §37 Q437) |

### 7.2 `laya-native`: in-process Rust, spike-gated (D527)

The desktop wants a CPU path without a multi-gigabyte Python environment. SO1 Task 0 settles it with measurements, in this order:
1. **kevala as a pinned git dependency** (Apache-2.0). Gates: the `laya` pack answers our golden set (`§9.2`, plus 200 labelled decisions) within 0.02 probability of `laya-serve` on the same inputs; native CPU p50 under 150 ms for a short state on the reference laptop (target, not a vendor figure); a multilingual pack exists or can be converted with its converter; the crate builds with our toolchain and `cargo deny` passes (it states zero dependencies). If it passes, `LayaNativeBackend` wraps its `Model` trait and the Python sidecar becomes the NVIDIA and server path only.
2. If kevala fails a gate: `ort` 2.0.0-rc (exact pin, `load-dynamic`) with `tokenizers` and our own port of Laya's request template and head, from the fp32 ONNX export. This is a multi-week port that must be re-verified on every upstream template change, so it needs the owner's go-ahead (Q523).
3. If neither: CPU users run the Python sidecar. That is acceptable and is the stated fallback.

### 7.3 `laya-coreml`: a Swift sidecar inside the desktop app (D528)

- Code lives in `ostrium-labs/loams-desktop` at `sidecars/coreml/` (a SwiftPM executable `loams-coreml`), not in this repository (it needs Xcode and macOS CI).
- It depends on `FluidInference/FluidUse` (Apache-2.0; its library product `FluidUse` includes `LayaManager`) **pinned by revision**. If building the whole package pulls in too much (FluidAudio and the demo targets), SO2 Task 0 vendors only the Laya files with their Apache-2.0 notice instead (Q525).
- It speaks the same HTTP/JSON as `laya-serve`, restricted to `POST /v1/systemone` and `GET /health`, on a **Unix domain socket** in `$LOAMS_HOME/run/` (mode 0600) with the same per-launch bearer key, so the Rust adapter is the same `LayaServeBackend` client over a different transport. No TCP port exists.
- Models: `FluidInference/laya-coreml` buckets are fetched by our model manager (not by `LayaManager.load()`'s own download, so verification and the cache policy are ours) and given to the sidecar by path. Default: the 128 and 512 buckets (what `LayaManager.load()` loads), int8-embedding variants where the model card shows equal accuracy, 256 and 1024 on demand. Compute units per bucket as FluidUse documents.
- Limits it enforces and reports: 32 options per `choice`, multilingual checkpoint only, a state over 1024 tokens truncated on the right with `usage.truncated`. The router already excludes it above 32 options (§6.3), and the sidecar returns `422` with `options_over_32` if asked anyway.
- macOS 14+ and Apple silicon only. The sidecar is code-signed and notarised with the app, in `Contents/MacOS/` or `Contents/Helpers/`, and launched by `loams-link`.
- Not shipped by upstream: no server exists, so this is ours, and it is the largest piece of new code in SO2.

### 7.4 `laya-mlx`: optional (D529)

`mizorewww/laya-mlx` (Apache-2.0, independent) runs the English **and** multilingual checkpoints on MLX. Its only real advantage over Core ML is the **English checkpoint on a Mac** (Core ML ships multilingual only), which is the accuracy gap in §6.3. It has no server and needs Python 3.11+. We do not depend on it in v1. If Q522 says the English gap matters, SO2 Task 9 adds an adapter that launches it behind the same `laya-serve`-shaped wrapper (a 150-line script we own), off by default, labelled "unofficial port" in the UI.

### 7.5 `laya-wasm`: the browser as a cordis plugin (D530)

- **Default in the browser is the server.** The console already talks to an instance; `POST /v1/systemone` returns in tens of milliseconds on a server GPU and nothing is downloaded.
- **Opt-in local inference** is a cordis plugin `@loams/plugin-systemone-local` (loaded from the catalog like any other, §37 D423): a dedicated Web Worker running **official `laya-ts`** on `onnxruntime-web` (WebGPU, then single-threaded WASM), with weights from our pinned mirror (§9.5). `kevala` (WASM plus WebGPU, needs no special headers, 479 MB pack) is the alternative if SO1's native spike picks it. `nvkudva/laya-web` is not used (no licence).
- **COOP/COEP.** Threaded WASM in onnxruntime-web needs cross-origin isolation (`Cross-Origin-Opener-Policy: same-origin`, `Cross-Origin-Embedder-Policy: require-corp`); without it the same note says about 6x slower. The Loams console **frames app UIs from sibling subdomains** (§39 D-SF-2, D-SF-3), and `require-corp` blocks any framed resource that does not send `Cross-Origin-Resource-Policy`. So the console itself must not turn COEP on. An iframe can only be cross-origin isolated if every ancestor is, so an isolated worker document **embedded in the console is not an option**. The compatible choices are: (a) a WebGPU or isolation-free engine in a plain Web Worker (kevala, or `laya-ts` on onnxruntime-web's WebGPU provider), (b) single-threaded WASM in a plain worker, about 6x slower (the figure is laya-web's), or (c) opening the model page as a **top-level isolated window** that the console talks to over a `MessageChannel`, which is awkward. v1 picks (a) with (b) as the fallback, and ships none of them unless the owner asks for in-browser inference (Q530).
- Main-thread execution (as laya-web does in production) would freeze the console for 340 ms to 2.4 s per call; the plugin always uses a worker.

## 8. The desktop integration (D538)

Loams Desktop is the zeron fork (§37 §18, `ostrium-labs/loams-desktop`). SystemOne enters it through `loams-link`, never through zeron's engine:

```
 Loams Bot (harness / bot-acp shim)      zeron mcp (injected tool)       Settings > Decisions
            │                                   │                              │
            └──────────────┬────────────────────┴──────────────────────────────┘
                           ▼
                 loams-systemone-link (in loams-link, no GPUI)
                  · HostProfile · Registry · Router (the crate `loams-systemone`, as a library)
                  · ModelManager (download, verify, cache, residency)
                  · SidecarSupervisor ── CoremlSidecarBackend ── UDS ──► loams-coreml (Swift, ANE)
                                      ├─ LayaServeBackend ── 127.0.0.1 + key ──► laya-serve (NVIDIA / CPU)
                                      ├─ LayaNativeBackend (in-process, if the spike passes)
                                      └─ RemoteBackend ── Connect ──► the instance's SystemOneService (when signed in)
```

- **The desktop links the `loams-systemone` crate** (small: no DataFusion or storage). That is allowed by §37 §18.3's rule against embedding the *engine*; this crate is a client library of 3 to 4 k lines, not the engine.
- **Local first, instance second.** With a local backend ready the desktop answers locally. With none ready, or by setting, it calls the instance's `SystemOneService` (the `RemoteBackend`), which is another registry. Data locality is shown in the UI per answer (`loams.locality`).
- **Models are the app's, kept resident.** The Model Manager downloads, verifies, caches and (setting) keeps the sidecar loaded; a lazy-start mode starts it on first use and stops it after N idle minutes. Resident memory is shown (Core ML fp16 bucket about 614 MB; torch English 808 MB, multilingual 647 MB per upstream).
- **Settings, "Decisions" panel** (GPUI, in `loams-panels`): the active backend and why (`route_reason`), a table of backends with status and a one-line fix for each `NotInstalled`, model sizes, download/verify/delete, residency, a "run self-test" button with the result, calibration status per question type, and a **Cloud decisions** section: off by default, a Jev key field stored in the OS keychain (never in config), the consent text from §10 and a per-org policy override from the instance.
- **Loams Bot uses it** (D538): (1) before dispatch, `route.agent` (choice over the five platform agents), `urgency` (score) and `needs_person` (noul) decide which A2A agent gets a message and whether to interrupt; (2) as a cheap pre-filter, "does this message need the LLM at all" (noul), which saves a model call for acknowledgements; (3) as the policy for notification priority on phones. The decision, its probabilities and the backend are shown on the thread's card ("routed to forgejo, 0.91") and sent into the A2A message metadata for tracing (`systemone.backend`, `systemone.latency_ms`, no content, §39 D-SF-11). **A SystemOne answer is advisory and never approves or authorises anything** (D537).
- **Agent surface.** A read-only MCP tool `systemone_decide` is added to the MCP server the engine injects into every run (as `zeron mcp` is), so coding agents can ask local questions; it has no network path and is off if no local backend is ready.
- **Telemetry.** The desktop sends nothing to Loams or anyone else (zeron has none; D284). Two separate things stay local: the decision log (§9.4), opt-in and off by default, and, when the desktop talks to an instance, the usage event `io.loams.dev.systemone.decided.v1` (§11) is emitted **by the instance's server path only** into that operator's own stream (counts, backend, latency, locality; never content), enabled when the operator runs the route; the local sidecar path emits no event. A2A trace metadata (`systemone.*`, no content) travels to the operator's own collector like every other §39 span.

## 9. Calibration, self-test, fine-tuning, models, cache

### 9.1 Calibration (D532)

The checkpoints are over-confident out of the box (§2.2). Loams fits a **temperature per (checkpoint, question type, option-count bucket)** on labelled decisions and applies it to the backend's probabilities (`p_i ∝ p_i^(1/T)`, equivalently logits divided by `T`), then computes `calibrated_confidence = max(p)` (for `noul`, `max(p, 1-p)`).
- Buckets by option count: 2, 3-4, 5-8, 9-20, 21-32, 33+ (a coarser version of upstream's "per type and option count"; the tool refuses a bucket with fewer than 100 labelled decisions and reports it as `uncalibrated`).
- `loams systemone calibrate --labels decisions.jsonl [--backend ...] [--out table.json]` fits by minimising negative log-likelihood, reports ECE before and after per bucket and writes a versioned table `cal-YYYY-MM-DD-<hash>.json` into `$LOAMS_HOME/systemone/calibration/`. Tables are per checkpoint **revision** and are invalidated when the revision changes.
- Defaults ship **no table**: answers are marked `calibration: ""` and `calibrated_confidence` equals the raw `answer_confidence`, with a documented warning. The first shipped table is a product of SO1's labelled set (§9.4) and is optional.
- Cloud backends are calibrated separately (Jev's published ECE is 0.246 raw; Laya's raw 0.213; our own measurement decides).
- `act_probability` is dropped from every backend's answer before calibration.

### 9.2 Self-test (D533)

A fixed set of **16 cases** embedded in the crate (state, questions, expected argmax or interval), covering: English choice with 3 options, a 12-option choice, a 40-option choice (`informational`, `requires: min_options 33`), score, `noul` true and `noul` false (the opposite pair for the stuck-label check), Hindi and German states (must route multilingual), a state at the window edge (truncation reported) and a JSON state. (The empty-options error is covered by validation tests.) Rules:
- Runs at backend start and after a model or revision change, in the background; the backend is `Starting` until it passes. It runs in under 2 s locally.
- It asserts structure always (shapes, probabilities sum to 1 within 1e-3, `output_tokens == 0`) and **argmax only on cases the base checkpoint is known to get right** (an allowlist we maintain from measurements, because the base checkpoints are below majority-class on hard benchmarks, §2.2). Each case declares `requires` (for example `min_options: 33`) and a backend that cannot serve it, such as Core ML on the 40-option case, reports it as `not_applicable`, never as a failure; the empty-options case is a validation test, not a backend case. A real failure sets `Degraded`, and `/health` and `loams systemone status` say which case.
- It is also the parity test between backends in CI.

### 9.3 Gating (D536)

`calibrated_confidence` is uniform across backends. `min_confidence` marks `low_confidence` and keeps the answer (Laya's behaviour); **callers that act on a decision must pass a threshold or handle abstention** (Loams Bot's default 0.6 until calibrated, then tuned). The backend-native `confidence` is returned for compatibility and never compared between backends. Which of `confidence` and `answer_confidence` predicts correctness better is measured on our labelled set (upstream says AUROC 0.77 for `confidence`, while its docs call `answer_confidence` the gating quantity, Q527).

### 9.4 Fine-tuning hooks (D534)

Zero-shot quality is modest and the base checkpoints sit below the majority-class baseline on hard tasks, so **fine-tuning is the product plan, not an afterthought**. We do not train inside the engine. We provide the hooks:
- **Decision log.** `systemone.log_decisions = off | local` (default off). When on, each decision is appended locally to `$LOAMS_HOME/systemone/decisions/YYYY-MM.jsonl` with the state **only if** `log_state = true` (default false: store a salted hash, the question set, the answer and the backend). Human corrections (Loams Bot's "wrong route" button, an approval that overrode the route) append a `label` row. Nothing leaves the machine; the file is the user's. Retention default 90 days.
- **Export.** `loams systemone export --format laya` writes the typed-decisions JSONL format that upstream's fine-tuning notebook (Kaggle 2x T4) expects, plus a held-out split for calibration.
- **Install.** `loams systemone model add --id acme-triage-1 --path ./ckpt --sha256 ...` registers a custom checkpoint (same architecture) in the lock (§9.5). The registry exposes it as `checkpoint: "acme-triage-1"`; `laya-serve` loads it by local directory. Core ML needs a conversion (FluidInference's script in `mobius`), which is outside v1.
- **First fine-tunes** (Q531): Loams Bot routing and urgency (agent choice, severity score, needs-person), because §39 needs them first. The labelling source is the factory's own history (an issue closed under a different label, an approval rejected).

### 9.5 Model download and cache policy (D535)

- **Location:** `$LOAMS_HOME/models/systemone/<backend>/<checkpoint>/<revision>/`. One copy per revision; a revision directory is immutable.
- **Lock:** `crates/loams-systemone/models.lock.toml`, compiled into the binary: per artefact its source (Hugging Face repo and **commit SHA**), file list with **SHA-256**, size, licence and `NOTICE` text. A mirror URL list follows (our RustFS bucket, Q535) so a Hugging Face outage or removal does not break installs. Updating the lock is a reviewed PR that reruns the self-test and parity tests.
- **Download:** only on user consent (CLI prompt or desktop dialog showing the size), resumable by byte range, written to `*.part`, SHA-256 verified **before** the atomic rename and before any parser touches the bytes, then `chmod -w`. A digest mismatch deletes the part and fails with `MODEL_CORRUPT`.
- **Sizes shown:** Laya English 808 MB, multilingual 647 MB (upstream), Core ML fp16 about 614 MB per bucket (the sidecar needs 1 to 2 buckets), `laya-ts` ONNX: not yet measured (the 524 MB int8 and 1 688 MB fp32 figures belong to `laya-web`, which we do not use; SO2 Task 7 measures the pinned `laya-ts` export), kevala Laya pack 479 MB.
- **Eviction:** never automatic; `loams systemone model rm`, and the desktop panel shows the disk use. Unused revisions older than the current and previous one are listed for removal.
- **Offline:** `LOAMS_OFFLINE=1` (and `HF_HUB_OFFLINE=1` for the sidecar) never downloads; `NotInstalled` carries the offline hint.
- **Licence handling:** weights are Apache-2.0; we download from the source, we do not bundle them in the installer by default (size), and the lock carries the required notice, shown in "About" and in `THIRD_PARTY_NOTICES.md`.

## 10. Jev, the cloud option (D531)

- **An adapter, not a dependency.** `JevBackend` posts to `https://api.typesafe.ai/v1/systemone` (configurable base URL, so also usable against `laya-serve`, `1Panel-dev/laya-server` or a private gateway), `Authorization: Bearer <key>`, body `{model, state, questions}` (the model defaults to `jev-latest`; `jev-1.13.0` and others may be pinned), and decodes `answers`, `model`, `usage`. It sends nothing else, no identifiers, no headers beyond the standard ones.
- **Off by default.** It is enabled by a key in the OS keychain (desktop) or the secret store (server), by an explicit setting, and, per call, by `locality: ALLOW_CLOUD`. Cloud is **never** an automatic fallback for private data; `LOAMS_SYSTEMONE_FALLBACK=jev` works only with `allow_cloud_fallback = true` at instance level (default false) and still honours the request's locality.
- **What the user is told** when enabling it: the state and questions are sent to TypeSafe; TypeSafe's Privacy Policy states it will not train on Input or disclose it beyond service providers; no API terms or retention period for API calls were found; the model is closed; the MCA's restrictions apply to the user (no standalone service, no distillation or training on Output); the price is $0.042 per million input tokens per third-party reports (unverified).
- **Failure handling:** 429 and 5xx back off with jitter and a circuit breaker; auth errors disable the backend and surface once; the request body is never logged.
- **Terms.** TypeSafe's Master Customer Agreement (2026-09-28) covers the API and allows a customer to include it in "software applications developed and operated by Customer", but forbids offering the Services "as a standalone service" and using Output for distillation or to train an imitating or competing model. The open questions are narrower than "no terms": does an OSS client with a user-supplied key, listed as a selectable backend, stay inside the licence, and may a Loams-hosted pass-through ever exist (our reading: no). The owner asks TypeSafe before the adapter ships enabled for anyone beyond the owner (Q520, Q521). Until then it is documented as "bring your own key; you accept TypeSafe's agreement". **Rule (D531): Jev outputs are never written to the decision log's label fields and never used as training or calibration labels.**
- **Transport.** The base URL must be `https` for any non-loopback host; plain `http` is accepted only for a loopback host (a local `laya-serve`). Redirects are not followed with the key attached: a redirect to a different host or scheme fails the request. The key is sent only to the configured base.

## 11. Open source versus commercial (D539, D220)

| Open (this repository, Apache-2.0) | Commercial (`loam-platform`, private) |
|---|---|
| `loams.systemone.v1`, the HTTP route, `loams-systemone`, every adapter including the Jev client (bring your own key) | A **Loams-hosted** SystemOne inference API for tenants (GPU fleet, scaling, pre-warming), if it is ever sold |
| The sidecar manager, model manager, lock file and mirrors config | Metering and billing of decision calls (OSS emits usage events only: `io.loams.dev.systemone.decided.v1` with counts, backend and latency, never content) |
| Calibration tool, self-test, decision log and export, custom checkpoints | Managed fine-tuning as a service and curated per-industry checkpoints sold as a product |
| The desktop integration, Loams Bot's use of it, the cordis plugin | Per-plan quotas on decisions; abuse and safety tooling on a hosted endpoint |

Self-hosting a single organisation with local backends is fully open; nothing in this repository depends on `loam-platform`.

## 12. Security model (D537)

1. **Loopback only for sidecars**, generated per-launch key, UDS for Core ML; the adapter refuses non-loopback addresses.
2. **Models are inert data, but loading them is parsing.** SHA-256 before parse; only safetensors/Core ML/ONNX/pack formats, never pickled `torch.save` from an unverified source (kevala's `torchpt.rs` reads `torch.save`: SO1 Task 0 checks it is only used by its converter, which we do not run on unverified files).
3. **State is untrusted text.** A classifier cannot execute instructions in it, but it can be steered: an attacker who controls the text of a ticket can push `urgency` or `route`. Therefore a SystemOne decision **may rank, route or prioritise; it may not approve, authorise, merge, delete, deploy or grant**. §39's gates (D-SF-9) are unaffected. A decision that selects an action with side effects still goes through the approval path.
4. **No content in telemetry.** Spans carry backend, latency, option counts, the decision and calibrated confidence; never the state.
5. **Resource limits.** Request limits (§4.2) before any backend; the sidecar queue depth; a per-principal rate limit on the server.
6. **Supply chain.** The Python environment is built from hash-pinned requirements we review; `uv` is pinned; Swift dependencies pinned by revision; kevala pinned by commit; `cargo deny` and the licence policy apply. No PgDog (AGPL) text anywhere.

## 13. Testing

Per plan task (see the plans). The cross-cutting ones:
- **Wire fixtures.** The Laya sample request and response from its HTTP API doc and Jev's documented shapes are golden files; our JSON parses them and renders responses that are **semantically equal** for the fields they define (parsed-value equality, with option members compared as sequences); byte equality is not claimed.
- **Routing table tests** (pure function): every rule in §6.3, with host profiles for each row of §1.
- **Fake `laya-serve`** (an axum test server obeying the documented limits and error codes) for the adapter and supervisor; one `#[ignore]` real-model test per backend, run on a labelled self-hosted runner.
- **Parity.** The self-test cases run through every available backend; probabilities agree within 0.02 on the same checkpoint (Core ML claims accuracy parity with PyTorch on 3 899 questions, to be re-measured).
- **Chaos.** Kill the sidecar mid-request; corrupt a downloaded file; fill the disk; unplug the network; a fallback to a disallowed cloud (must fail closed).

## 14. Roadmap

| Plan | Contents | Depends on |
|---|---|---|
| **SO1** | Spike (kevala, `ort`, `laya-serve` measurements); protos and mock; `loams-systemone` crate (types, wire, trait, registry, host detection, routing); `laya-serve` adapter and supervisor; Jev adapter; calibration; self-test; model manager; server route, CLI and MCP; decision log; docs and exit gate | AP0's proto and Connect conventions (D128); `loams-link` unaffected |
| **SO2** | Reconcile with `loams-desktop`; the Swift Core ML sidecar; `loams-systemone-link` in the app; Model Manager and Settings panels; Loams Bot integration; zeron MCP tool; optional cordis browser plugin; Windows and Linux CPU path; optional MLX; exit gate | SO1 Tasks 1 to 8, AP1n Task 4 (Connect client in `loams-link`), SF3 Task 5 (the desktop harness) |

The track is **SO** (new label `track:so`, "SystemOne").

## 15. Risks

| # | Risk | Mitigation |
|---|---|---|
| 1 | **Zero-shot quality is below the majority-class baseline** on hard tasks; shipping uncalibrated, un-fine-tuned decisions would make Loams Bot worse | Labelled evaluation sets for our first tasks before any default-on use; `min_confidence` mandatory in Loams Bot; fine-tune first (§9.4); ship SystemOne as "available" and flip it on per decision type only after it beats the baseline in our own data |
| 2 | A single maintainer and a very young repository (kevala, v0.1.3); an independent MLX port; unofficial Core ML conversion | Everything is behind the trait; pinned commits; parity tests against `laya-serve`; the Python sidecar remains a complete fallback |
| 3 | Vendor figures (3.7 ms, 32.8 ms) are not ours | SO1 Task 0 and SO2 Task 0 re-measure on our hardware; nothing in the UI promises a number |
| 4 | Python/PyTorch weight and platform quirks on the desktop (the `USE_TF=0` hang, CUDA OOM falling back to CPU, torch wheel selection) | Used only on NVIDIA and server hosts by default; `/health` degradation is surfaced; `USE_TF=0` set by the supervisor |
| 5 | Over 20 options is the known weak case | Router warns; widen budget; Jev (opt-in); two-step hierarchy is a follow-up (Q528) |
| 6 | Cloud use leaks private data | Local-only default at three levels (instance, request, backend); fail-closed fallback; consent copy; tests |
| 7 | Hugging Face outage, removal or a force-pushed revision | Pinned SHAs, our mirror, digests |
| 8 | An unlicensed or restrictive third-party port slips in | Licence table (§2.1) is a CI check for `THIRD_PARTY_NOTICES.md` entries; `nvkudva/laya-web` banned by name |
| 9 | COEP/COOP for in-browser WASM conflicts with framed app UIs | Server path by default; no COEP on the console; WebGPU worker or single-threaded WASM; Q530 |
| 10 | "System One" and "SystemOne" are TypeSafe's wording; `/v1/systemone` is their path | Our feature name is Loams SystemOne (the owner's name); path compatibility is deliberate and documented as wire compatibility; Q537 asks for a trademark check |
| 11 | Jev terms: the MCA forbids a standalone service and distillation, and the OSS-client question is open | Off by default; no pass-through; Jev outputs never used as labels; owner action Q520 |
| 12 | Model memory on small laptops (Core ML 614 MB per bucket, torch 808 MB) | Lazy residency, idle stop, int8 variants, a limit shown in settings |

## 16. Contradictions with earlier decisions, and how they are resolved

- **§37 §18.3 "do not embed the engine in the app".** The desktop links the small client crate `loams-systemone`, not the engine; unchanged in spirit (D538).
- **D220 (hosted or commercial inference goes in `loam-platform`).** A Jev client adapter is a client of someone else's API with the user's own key and is open; a Loams-hosted inference service is commercial (§11).
- **D111 (loopback only until auth).** The sidecars and the desktop route are loopback; the server route sits behind normal auth.
- **§39 D-SF-11 (no content in spans).** Kept (D537.4).
- **§37 §5.6 trust tiers.** The browser plugin is a first-party plugin; its worker document is a separate origin by design (§7.5).

## 17. Open questions

Q520 to Q539 are in [decision log](13-decision-log.md). The ones that block work: Q520 and Q521 (Jev terms and keys, owner), Q522 (English quality on Apple silicon), Q523 (kevala versus our own `ort` port, SO1 Task 0), Q525 (FluidUse packaging).

## 18. Sources

All fetched or queried on 2026-10-02.
- Laya: huggingface.co/convaiinnovations/laya (card, API metadata, files); github.com/NandhaKishorM/laya (`LICENSE`, `README.md`, `docs/http-api.md`, `docs/security.md`, `docs/typescript-sdk.md`, `laya-ts/`, `scripts/export_onnx.py`); pypi.org/project/laya.
- Core ML: huggingface.co/FluidInference/laya-coreml (card, API metadata, file list); github.com/FluidInference/FluidUse (`README.md`, `Package.swift`).
- MLX: github.com/mizorewww/laya-mlx; huggingface.co/aac6fef/laya-mlx (licence via API).
- Browser: github.com/nvkudva/laya-web (no licence); github.com/bvolpato/kevala (`README.md`, `docs/architecture.md`, `docs/models.md`, `docs/packs.md`, `BENCHMARK.md`, `Cargo.toml`); github.com/vishalmysore/layaForWeb.
- Self-hosted API: github.com/1Panel-dev/laya-server (Apache-2.0).
- Jev: docs.typesafe.ai (overview and primitives), docs.litellm.ai/docs/pass_through/typesafe, typesafe.ai/legal/terms and /legal/privacy-policy, github.com/browser-use/jev-ultrafast (MIT).
- Rust: crates.io `ort` (2.0.0-rc.13, 2026-07-28), `kevala` (absent); `huggingface/candle` (`modernbert.rs`).
- Internal: §37 (D422, §18), §39 (D-SF-2, D-SF-3, D-SF-9, D-SF-11), `docs/open-core.md` (D220), D111, D128.
