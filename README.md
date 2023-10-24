# Gomer

Gomer is a service framework that provides a "right by default" framework for CRUD+L resource-oriented APIs and 
aims to remove as much boilerplate code as possible. Probably the easiest way to understand what Gomer offers is by
example. For that, let's take the semi-canonical "Beer Service" example and update it to be a bit more full-featured
and turn it into a multi-tenant service. We'll call it "AWS Brewski".

The AWS Brewski service allows customer's to create and manage their stores, define and update the beers each store
carries, and keep track of the inventory available for those beers. Let's start by defining the `Beer` resource.

```go
type Beer struct {
    AccountId  string
    Identifier string
    Name       string
    Style      string
    Stock      int
}
```

Most of the fields in `Beer` are self-explanatory, though `AccountId` may prompt a question. It's there of course
because the service is multi-tenant and we need to disambiguate one customer's resources from another. That makes
sense when thinking about the internal model, but what about the external model? In Gomer, the struct is used
for both! While we often hear about the "best practice" of keeping your API model and your data model separate from
each other and allowing each to evolve independently, in practice the two are almost always highly correlated and
evolve together in a synchronized fashion. So while you could have two models (which Gomer can support), it's 
simpler to have just one model until non-functional requirements dictate otherwise.

### Binding

Back to Brewski. Each operation the service exposes on the `Beer` resource needs to define what the input and 
output representation should be. Gomer supports this via a mechanism that takes externally provided data, such as 
JSON data sent over the wire, and binds it to the resource's structure (an "in" binding) and from the resource 
back to an external representation (an "out" binding). These binding definitions exist alongside the 
resource's definition using Go "struct tags" (similar to Java field annotations). Each struct tag contain one 
or more entries in the form of `key:"value"`, where each key defined by Gomer corresponds to a type of "struct tool".
Gomer terms the key's value a "directive", and it defines how the tool should be applied to that particular field.

Let's start with some simple binding directives, but with some slight variations to illustrate several points.

```go
type Beer struct {
    AccountId  string `... out:"-"`
    Identifier string `... out:"id"`
    Name       string `in:"Name" out:"Name"`
    Style      string `in:"+" out:"+"`
    Stock      int    `in:"+" out:"Stock"`
}
```

Starting with `Name`, the `in` and `out` tags contain a simple string, which Gomer interprets to mean the name of a 
key where the field's value (coming from some input source) can be found and where the value should be written when 
rendered for output. Since it's common for the attribute and field names to match, the "+" directive is available as
a shorthand form. `Style` and the `Stock` `in` tag use that form.

As seen with `Identifier`, it's more common to explicitly provide an input or output name directive when the API and 
resource field's name shouldn't match.

To exclude a field being included with the output, one can explicitly use the "-" directive (see `AccountId`) for 
the `out` tag, or one can just omit the tag altogether.

You may have noticed that the `Identifier` field's `out` name is lower-cased (matching a camelCase style) 
whereas the `Name` and `Stock` field's `out` names are upper-cased (matching a PascalCase style). Despite the
inconsistency, Gomer will fix that up (default is to use PascalCase, but camelCase can be configured).

#### Functions

In addition to binding data from a request payload, Gomer allows you to invoke a function to produce the data needed 
to populate a field's value. Take this updated version of our resource.

```go
type Beer struct {
    AccountId  string `in:"$accountId"`
    Identifier string `... out:"id"`
    Name       string `in:"+" out:"+"`
    Style      string `in:"+" out:"+"`
    Stock      int    `in:"+" out:"+"`
}
```

`$accountId` corresponds to a function that's defined in the `GoAmzn-GomerAwsService` package and is able to extract 
the caller's account id from the request's sigv4 signature. In general, directives that start with the `$` symbol 
correspond to function or function-like behavior.

#### Defaults and composites

Sometimes we want to provide a default value for an input or output field. For example, suppose we introduce a 
`Badges` feature that represents achievements or attributes for a beer. When a beer is first created by a customer in 
the service, we automatically put a "New" badge on it.

```go
type Beer struct {
	...
    Badges string `in:"=New" out:"+"`
}
```

More practical given our current resource model, we decide that the `Style` attribute should always have a value 
whether the customer provides one or not. If they do provide one, though, we don't want to overwrite it. To support 
that, we can use one of the binding tool's composition options:

```go
type Beer struct {
    AccountId  string `in:"$accountId"`
    Identifier string `... out:"id"`
    Name       string `in:"+" out:"+"`
    Style      string `in:"+?=undefined" out:"+"`
    Stock      int    `in:"+" out:"+"`
}
```

The directive works by first applying the `+`, which will fill in the value with the whatever is passed in with 
the request. The `?` composition tests to see whether the field's value has been set or not. If yes, the 
right-hand side of the directive is ignored. If no, the right-hand side is applied, which in this case sets the 
value to "undefined".

#### Scopes



#### Extensions


### Validation

Since the `Beer` struct will contain data that we receive from customers, we'd like to make sure the fields meet
the criteria we define for them, such as a maximum length for the beer's name or that the stock value isn't
negative. Gomer allows us to specify each field's constraints alongside the resource definition using Go "struct tags".

```go
type Beer struct {
    AccountId string
    Id        string `validate:"regexp(^[a-z0-9]{8}$)"`
    Name      string `validate:"len(1,64)"`
    Style     string `validate:"oneof(ipa,pilsner,lager,ale,stout,unspecified)"`
    Stock     int    `validate:"gte(0)"`
}
```

There are ~45 different constraint types built into Gomer, plus you can define your own if needed.

```go
type Beer struct {
	...
    TargetStock  int `validate:"gte(0)"`
	MaximumStock int `validate:"int(gte,$.TargetStock)"`
}
```



It's easy to see how these map to the constraints one might define in a Smithy model. Gomer, though,
provides more advanced validation behaviors as well. For example,


```smithy
$version: "2"
namespace aws.brewski

service Brewski {
    version: "2006-03-01"
    resources: [Beer]
}

resource City {
    identifiers: { cityId: CityId }
    
    read: GetCity
    list: ListCities
    resources: [Forecast]
}
```
