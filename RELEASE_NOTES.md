## Release Notes

### 0.3.1

- resource: Add `Retry` hook to action lifecycle with up to 2 automatic retries; `Instance` gains `RetryCreate`, `RetryRead`, `RetryUpdate`, `RetryDelete` and `Collection` gains `RetryList` (default: propagate error)
- api/rest: Remove `ErrRenderer` chain from `ResponseWriter`; error rendering is now handled via `RenderErrorMiddleware`
- api/rest: `WriteError` no longer accepts optional renderers
- api/rest: Fix `defaultErrorRenderer` to use `gomerr.ErrorAs` instead of type assertion, and pass pointer to `json.Unmarshal`
- api/rest: Add `StatusCode()` and `Body()` accessors to `ResponseWriter`
- data/dynamodb: Refactor uniqueness constraint to use `constraint.Constraint` abstraction and resolve existing records via `persistableType.resolver`
- data/dynamodb: Populate query results on uniqueness violations for downstream consumption
- gomerr: `Conflict()` now takes `(with, id, problem string)` instead of `(with any, problem string)`; `ConflictError.With` changed from `any` to `string` with new `Id` field
- structs: Normalize error attribute keys to lowercase (`"Field"` → `"field"`, `"Value"` → `"value"`)
- auth: Normalize `fieldAccessScope` value to lowercase `"field"`
- deps: Bump versions

### 0.3.0

- api/rest: Preprocess structs during route building to validate config and ensure correct applier semantics
- data/dynamodb: Support partial updates of nested properties (previously required full replacement).
- data/dynamodb: Add support for retrieving a resource by alternative identifier value such as `name` instead of `id`.
- structs: `DirectiveProvider` now handles absent versus empty directives
- provider (breaking): `provider.SingletonProvider` renamed to `provider.Singleton`
