# Contributing

> “A foolish consistency is the hobgoblin of little minds, adored by little
> statesmen and philosophers and divines. With consistency a great soul has
> simply nothing to do. He may as well concern himself with his shadow on the
> wall." -- Ralph Waldo Emerson

The rules and conventions outlined below are an attempt to keep a clean and
sane working environment within our repos.  Work that doesn't adhere to the
stated standards should / will be rejected until it does.

## Don't commit code directly to the main branch

Period.

Use merge requests for your work, and have at least one maintainer on the
repository review your work (or, if you're the only maintainer, find someone
else to help review your work) before you merge anything.

See [our more in depth guidelines](https://www.notion.so/tangramvision/Contributing-to-repositories-at-Tangram-f9ba1c783ae94c1d9211d6c545586caa)
on Notion for more information.

### Creating new branches

Keep your branch names meaningful. If you're implementing a feature, try to use
the name of that feature as the branch name. It is also helpful if you can
prefix your branch with either your name (e.g. `jeremy/`) or some other folder
prefix. Gitlab does a good job of sorting those into groups / folders, so it is
convenient if we have several live branches going.

### Before you submit an MR

Tangram mandates that you run `rustfmt` when saving your source files, and
utilize clippy (`cargo clippy`) to lint your code before your MR is ready to
merge. There is typically some format / linting process for other languages as
well, so try to use those and encourage others to do the same!

If clippy fails in our pipeline, you'll be asked to go back and fix any of the
linter errors (or, in the rare circumstance the linter is wrong, at least
silence it).

## Commit messages

A repo with a strong culture of great commit messages is a wonderful thing.
Commit messages are the number one way of communicating intent and context
across a project. If you have a well-organized history, it is fairly
straightforward to ascertain the context behind why code is written the way it
is. From this, it is usually much easier to fix a bug, or get in the mindset of
whoever wrote the code you're looking at (even if that person is you).

See [our Git commit guidelines](https://www.notion.so/tangramvision/Git-tips-tricks-17118fa4427a45f78b2b72b80146cc4b)
on Notion for more information.

**NOTE**: This point is very important! You will have your bad commits called
out, and you will be asked to fix them.
