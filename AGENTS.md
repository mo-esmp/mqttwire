# Agent Guidelines

## Code Style

Follow `.editorconfig`; these supplement it:

* File-scoped namespaces; a file's namespace is its full folder path (`MqttWire.Codec`). Project-wide `global using`s only in the project's `GlobalUsings.cs`.
* Primary constructors for services, handlers and view models; constructor injection only.
* Name the type on the left, infer it on the right: never `var`, and target-typed `new` (`ConversationService service = new();`) — both are build errors. Where the left names no type, spell it out (`return new ChatResponse();`, `List<string> layers = [];`).
* Private static fields take `s_`. Async methods end in `Async` (a Mediator handler's `Handle` excepted); never `.Result` or `.Wait()`. Expression-bodied members for one-liners; braces on every control statement; guard clauses and early return; a blank line after each property when a class has more than one.
* Public APIs must have XML documentation. Avoid one-line XML doc comments; use the multi-line form:

  ```csharp
  /// <summary>
  /// This is a sample
  /// </summary>
  ```

## Testing

* xUnit + Shouldly + NSubstitute; AAA comments in every test. Never call a real provider from a unit test.

## Branches & Pull Requests

* Branch: `feature/<issue-number>-<brief-issue-title>`, kebab-case, e.g. `feature/233-attachment-entity-options-switch`.
* Pull request title: `#<IssueNumber> - <IssueTitle>.` — the squash commit on `main` takes it, with the PR number appended.
