# TODO and GitHub Issue Automation

This repository uses git hooks to automatically sync between `TODO.md` and GitHub issues.

## How It Works

### Commit Message Keywords

Use these keywords in your commit messages to automatically manage issues:

**Close an issue:**

```bash
git commit -m "Fixes #3: Add exponential backoff"
git commit -m "Closes #5: Fixed memory leak"
git commit -m "Resolves #7: Implemented dynamic table creation"
```

**Reference an issue (adds a comment):**

```bash
git commit -m "Updates #6: Added initial rolling window support"
git commit -m "Refs #8: Investigating streaming insert API"
git commit -m "See #10: Research for rebalancing"
```

### What Happens Automatically

1. **During commit** (`commit-msg` hook):
   - Parses your commit message for issue references
   - Prepares to update issues after commit

2. **After commit** (`post-commit` hook):
   - Marks completed items in `TODO.md` with `[x]`
   - Comments on GitHub issues with commit details
   - Stages `TODO.md` if it was modified

### Manual TODO Management

You can also manually update `TODO.md`:

```markdown
- [x] **Completed task** [#3](link) - Mark with [x]
- [ ] **Pending task** [#5](link) - Keep as [ ]
```

Then commit:

```bash
git add TODO.md
git commit -m "Update TODO: mark tasks complete"
```

## Setup

The hooks are already configured via Husky. If you need to reinstall:

```bash
npm install
npx husky install
```

## Requirements

- **Git hooks**: Automatically installed via Husky
- **GitHub CLI** (optional): Install for automatic issue commenting
  ```bash
  brew install gh  # macOS
  gh auth login
  ```

Without GitHub CLI, hooks will skip issue commenting but still update `TODO.md`.

## Examples

### Example 1: Completing a task

```bash
git commit -m "Fixes #3: Implemented exponential backoff for BigQuery errors

Added configurable retry strategy with exponential backoff and jitter.
- Max 5 retry attempts
- Backoff starts at 1s, doubles each retry
- Added logging for retry attempts"
```

**Result:**

- ✅ TODO.md updated: `- [x] **Add exponential backoff...** [#3]`
- 💬 Issue #3 gets a comment with commit details
- 🏷️ Issue #3 ready to be closed manually (or via PR)

### Example 2: Work in progress

```bash
git commit -m "Updates #6: Add basic rolling window structure

Started implementing rolling window for multi-table mode.
Still TODO:
- Backfill logic
- Cleanup/retention"
```

**Result:**

- 💬 Issue #6 gets a progress update comment
- 📋 TODO.md stays as `[ ]` (not completed yet)

### Example 3: Multiple issues

```bash
git commit -m "Updates #7 and #8: Research streaming APIs

Investigating both dynamic table creation and streaming inserts.
See issue discussions for details."
```

**Result:**

- 💬 Both issues get update comments
- 📋 Both stay incomplete in TODO.md

## GitHub Actions Integration

To automatically close issues when PRs are merged, use keywords in PR descriptions:

```markdown
Closes #3
Fixes #5
Resolves #7
```

GitHub will automatically close these issues when the PR is merged.

## Best Practices

1. **One issue per commit** for clarity
2. **Use descriptive commit messages** - they appear on issues
3. **Don't use "Fixes" until actually complete** - use "Updates" for WIP
4. **Keep TODO.md and issues in sync** - hooks help but review periodically

## Troubleshooting

**Hooks not running?**

```bash
ls -la .husky/
npx husky install
```

**GitHub CLI not authenticated?**

```bash
gh auth status
gh auth login
```

**TODO.md not updating?**

- Check that issue numbers match exactly
- Verify `sed` is available (standard on macOS/Linux)
- Manual fallback: Edit TODO.md directly

---

**Note:** These are local hooks. They update your local TODO.md and comment on issues using your GitHub credentials. Always review changes before pushing!
