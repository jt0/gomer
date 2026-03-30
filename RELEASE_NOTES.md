## Release Notes

### 0.3.0

- api/rest: Preprocess structs during route building to validate config and ensure correct applier semantics
- data/dynamodb: Support partial updates of nested properties (previously required full replacement).
- data/dynamodb: Add support for retrieving a resource by alternative identifier value such as `name` instead of `id`.
- structs: `DirectiveProvider` now handles absent versus empty directives
- provider (breaking): `provider.SingletonProvider` renamed to `provider.Singleton`
