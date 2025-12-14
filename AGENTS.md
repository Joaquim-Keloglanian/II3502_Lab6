# Git Commit Message Format

```
git commit -m "<type>(operational scope): <title>.

<bullet point>
<bullet point>
<bullet point>
.
.
.

Refs: #<local branch name>
```

# Pre-commit Actions

- Run uv run ruff format
- Run uv run ruff check
- Run all tests
- Fix code (not the tests, unless the tests are somehow flawed)
- Repeat until code is working
