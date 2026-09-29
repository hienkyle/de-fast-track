# Unit 01 — Version Control with Git: Study Guide

**Scope:** Git fundamentals (commit, push, pull, branch, merge, rebase) · Collaborative workflows (pull requests, code reviews) · Repository management best practices

This unit is about Git itself, so the commands are part of the material and are kept on purpose.

---

## Part A — Why Version Control Matters

**Version control** records every change to a project, so you can see **who** changed **what**, **when**, and **why**, and go back to any earlier state.

**Git is distributed:** every developer has a **full copy of the repository and its history** on their own machine. Most work happens locally, and you sync with a shared **remote** (GitHub, GitLab, Azure Repos) when needed.

In data engineering, Git holds pipeline code, SQL transformations, dbt models, Airflow DAGs, infrastructure configs and tests. It lets a team change production pipelines safely and roll back when something breaks.

---

## Part B — The Core Mental Model

### 1. Three local areas and one remote

| Area | What it holds |
|---|---|
| **Working directory** | The files you're editing right now |
| **Staging area (index)** | Changes you've marked to go into the next commit |
| **Local repository** | Your committed history, stored in the `.git` folder |
| **Remote repository** | The shared copy others push to and pull from (usually named `origin`) |

Changes flow: **edit → `git add` → `git commit` → `git push`**

### 2. Key terms

| Term | Meaning |
|---|---|
| **Commit** | A snapshot of the project at one point in time. Each commit has a unique **hash** (e.g. `a3f9c21`), an author, a message, and a pointer to its **parent** commit(s). |
| **Branch** | A lightweight, movable **pointer to a commit**. Creating a branch doesn't copy files. It just creates a new pointer. |
| **HEAD** | A pointer to where you are now, usually the tip of the branch you have checked out |

---

## Part C — Git Fundamentals

### 1. Commit: saving snapshots

```bash
git status                  # see what changed and what is staged
git diff                    # see unstaged changes line by line
git add file.py             # stage one file
git add .                   # stage all changes in the current folder
git commit -m "Add null check to orders ingestion"
git log --oneline --graph   # view history compactly
```

Staging exists so you can commit **related changes together** and leave unrelated ones for a separate commit.

### 2. Push: sending commits to the remote

```bash
git push origin main
git push -u origin feature/add-dedup   # first push of a new branch; -u sets upstream tracking
```

A push is **rejected** if the remote has commits you don't have yet. Pull first, then push.

### 3. Pull: getting others' commits

```bash
git fetch    # download remote changes without touching your files
git pull     # fetch + merge (or fetch + rebase, if configured)
```

**Key distinction:** `fetch` only **downloads**. `pull` downloads **and integrates** the changes into your current branch.

### 4. Branch: working in isolation

```bash
git branch                     # list branches
git branch feature/new-dag     # create a branch
git switch feature/new-dag     # move to it (older: git checkout)
git switch -c feature/new-dag  # create and switch in one step
git branch -d feature/new-dag  # delete a branch once it's merged
```

Branches let you build a feature or fix without destabilizing `main`, which should always stay working and deployable.

### 5. Merge: combining branches

```bash
git switch main
git merge feature/new-dag
```

| Merge type | When | Result |
|---|---|---|
| **Fast-forward** | `main` hasn't moved since you branched | Git just slides the `main` pointer forward. **No new commit.** |
| **Three-way** | Both branches have new commits | Git combines them using their **common ancestor** and creates a **merge commit with two parents** |

### 6. Merge conflicts

A conflict happens when two branches change **the same lines of the same file**, or one edits a file the other deletes. Git marks the section:

```
<<<<<<< HEAD
WHERE status = 'active'
=======
WHERE status IN ('active', 'pending')
>>>>>>> feature/new-dag
```

- **To resolve:** edit the file to what you actually want, remove the markers, then `git add <file>` and `git commit` (or `git merge --continue`).
- **To back out:** `git merge --abort`.

### 7. Rebase: moving your work onto a new base

Rebase is the other way to bring two branches together. Instead of joining the two histories with a merge commit, it moves your branch so it looks like you **started from the latest version** of the other branch.

You branched `feature` off `main` at commit E and made A, B, C, while teammates added F and G to `main`:

```
        A---B---C   feature
       /
  D---E---F---G     main
```

**Merge** `main` into `feature`: Git creates a merge commit M that joins both lines.

```
        A---B---C---M   feature
       /           /
  D---E---F---G---      main
```

**Rebase** (`git rebase main` while on `feature`): Git

1. temporarily sets aside your commits A, B, C,
2. moves `feature` to point at G (the tip of `main`),
3. re-applies your changes one at a time on top of G, creating **new** commits A', B', C'.

```
                A'--B'--C'   feature
               /
  D---E---F---G              main
```

The result is a straight, linear history with no merge commit. Merging `feature` into `main` afterwards is a simple fast-forward.

⚠️ **The most important detail:** A', B', C' are **new commits with new hashes**. They contain the same changes as A, B, C, but their parent is different, and a commit's hash depends on its parent. This is what "rebase **rewrites history**" means.

```bash
git switch feature
git rebase main       # replay feature's commits on top of main
```

**Why a force push is needed:** if you rebase a branch you had already pushed, your local branch and the remote no longer share the same commits, so a normal push is rejected. Use `--force-with-lease` rather than `--force`: it **refuses to overwrite** the remote if someone else pushed there since you last fetched.

```bash
git push --force-with-lease
```

**Merge vs rebase:**

| | **Merge** | **Rebase** |
|---|---|---|
| History | Preserves exactly what happened, including when branches split and rejoined | Linear and easier to read |
| Commits | Adds a merge commit | Rewrites your commits with new hashes |
| Safe on shared branches? | ✅ Yes | ❌ No, only on branches nobody else is using |
| Typical use | Integrating a finished PR into `main` | Updating your own feature branch with the latest `main` |

⚠️ **The golden rule:** never rebase commits that other people have already pulled. If a teammate built on your original A, B, C and you replace them with A', B', C', their history no longer matches yours, and they get **duplicated commits and confusing conflicts** on their next pull. Rebasing your **own, unshared** feature branch is fine and common.

### 8. Undoing things

| Command | What it does | Safe on shared history? |
|---|---|---|
| `git restore <file>` | Discard unstaged changes to a file | ✅ Yes (local only) |
| `git restore --staged <file>` | Unstage a file but keep the edits | ✅ Yes |
| `git commit --amend` | Rewrite the most recent commit | ⚠️ Only if not yet pushed |
| `git revert <hash>` | Create a **new** commit that undoes an old one | ✅ **Yes — the preferred choice** |
| `git reset --hard <hash>` | Move the branch back and **discard** later work | ❌ No — destructive |

---

## Part D — Collaborative Workflows

### 1. Feature branch workflow (GitHub Flow)

1. Pull the latest `main`.
2. Create a branch for **one** feature or fix (e.g. `feature/customer-dedup`, `fix/null-timestamps`).
3. Commit small, logical changes and push the branch.
4. Open a **pull request (PR)** to merge it into `main`.
5. **Automated checks** run (tests, linting, SQL validation) and teammates **review** the code.
6. Address feedback by pushing more commits to the same branch. The PR updates automatically.
7. Once approved, merge and **delete the branch**.

### 2. Pull requests

A **PR** is a request to merge one branch into another, packaged with a place for discussion.

A good PR has:

- a clear title,
- a description of **what** changed and **why**,
- a link to the related ticket or issue,
- notes on how it was tested,
- a **small, focused scope** (large PRs get weaker reviews).

**Merge options** on most platforms:

| Option | Result |
|---|---|
| **Merge commit** | Keeps every PR commit plus a merge commit |
| **Squash and merge** | Combines all the PR's commits into one |
| **Rebase and merge** | Replays the PR's commits onto `main`, with no merge commit |

### 3. Code reviews

**Reviewers check:** correctness, readability, tests, performance, and security (e.g. hardcoded credentials, missing error handling, a query that scans an entire table).

**Review etiquette:**

- Comment on the code, not the person.
- Explain the reasoning behind a suggestion.
- Separate **blocking issues** from minor nitpicks.
- As the author, respond to every comment and keep the PR easy to review.

---

## Part E — Repository Management Best Practices

### 1. Commits

- Make each commit **one logical change**.
- Write messages in the **imperative mood** that explain **why**, not just what (e.g. "Fix duplicate rows in daily sales load").
- Commit often, but never commit broken code to `main`.

### 2. Branches

- Use consistent naming: `feature/`, `fix/`, `hotfix/`.
- Keep branches **short-lived** to reduce conflicts.
- Protect `main` with **branch protection rules**: required PR reviews, required passing CI checks, and **no direct pushes or force pushes**.

### 3. What NOT to commit (especially in data engineering)

| Don't commit | Why / what to do instead |
|---|---|
| **Secrets**: passwords, API keys, connection strings | Use environment variables or a secrets manager. ⚠️ If a secret is ever committed, treat it as **leaked and rotate it**. Deleting it in a later commit doesn't remove it from history. |
| **Large data files** (CSV/Parquet exports) | Git is built for code, not datasets. Keep data in object storage, or use **Git LFS** or **DVC** if it needs versioning. |
| **Generated files**: virtual environments, logs, `__pycache__`, notebook checkpoints (`.ipynb_checkpoints/`) | Rebuilt automatically, so they only add noise. Exclude them with `.gitignore`. |
| **Notebook outputs** | ⚠️ These are saved **inside** the `.ipynb` file itself, so `.gitignore` can't exclude them. Clear outputs before committing, or use a tool such as `nbstripout` that strips them automatically. |

A `.gitignore` excludes files automatically:

```
.env
venv/
__pycache__/
*.log
data/
.ipynb_checkpoints/
```

### 4. Repository hygiene

- Include a **README** explaining what the repo does and how to run it.
- Use **tags** to mark releases (`git tag v1.2.0`).
- Pair the repo with **CI/CD** so tests run on every PR.
- Delete merged branches.

---

## Key Terms Cheat Sheet

- **Version control:** a record of every change: who, what, when, why
- **Distributed:** every clone has the full history
- **Working directory / staging area / local repo / remote:** your edits / changes chosen for the next commit / committed history / the shared copy (`origin`)
- **Commit:** a snapshot with a hash, author, message and parent(s)
- **Branch:** a movable pointer to a commit. **HEAD:** where you are now.
- **`fetch` vs `pull`:** download only / download and integrate
- **Fast-forward vs three-way merge:** move the pointer / new merge commit with two parents
- **Merge conflict:** both branches changed the same lines. Resolve, `add`, `commit` (or `merge --abort`).
- **Rebase:** replay your commits on a new base, creating **new hashes** (rewrites history)
- **Golden rule:** never rebase commits others have pulled
- **`--force-with-lease`:** a force push that refuses to overwrite others' new work
- **`revert` vs `reset --hard`:** new undo commit (safe) / move the branch back and discard (destructive)
- **`commit --amend`:** rewrite the last commit, only before pushing
- **GitHub Flow:** branch → PR → checks + review → merge → delete branch
- **Merge options:** merge commit, squash and merge, rebase and merge
- **Branch protection:** required reviews and CI checks, no direct or force pushes to `main`
- **`.gitignore`:** excludes secrets files, environments, data, logs, caches, notebook checkpoints
- **Notebook outputs:** live inside `.ipynb`, so clear them (or `nbstripout`) instead of relying on `.gitignore`
- **Leaked secret:** rotate it. Deleting it later doesn't remove it from history.
- **Git LFS / DVC:** for large files and data versioning
- **Tags:** mark releases (`v1.2.0`)

---

## Practice Questions

1. **What is the difference between the working directory, the staging area, and the local repository?**

    The working directory holds your current edits. The staging area holds changes selected for the next commit. The local repository holds the committed history.

2. **What does `git pull` do that `git fetch` does not?**

    `pull` also integrates the downloaded changes into your current branch (by merge or rebase). `fetch` only downloads.

3. **You branched from `main`, made three commits, and no one else touched `main`. What kind of merge happens, and is a merge commit created?**

    A fast-forward merge. No merge commit is created: Git just moves the pointer forward.

4. **Your push is rejected because the remote contains work you don't have. What should you do?**

    Pull (or fetch and merge/rebase) the remote changes, resolve any conflicts, then push again.

5. **You pushed a commit that broke a production pipeline, and teammates have already pulled it. `git reset` or `git revert`? Why?**

    `git revert`. It adds a new commit that undoes the change without rewriting shared history. `reset` would rewrite history that others already have.

6. **A teammate committed a database password, then deleted it in the next commit. Is the problem solved?**

    No. The password is still in the repository's history. It must be rotated, and the history cleaned if needed.

7. **After `git rebase main` on your feature branch, why do your commits have different hashes?**

    Rebase re-applies your changes on top of a new parent commit. A commit's hash depends on its parent, so the result is new commits with new hashes, even though the changes are the same.

8. **Why is it risky to rebase a branch other people are working on?**

    Rebase replaces the original commits with new ones. Collaborators' copies still have the old commits, so their history diverges from yours, causing duplicated commits and confusing conflicts.

9. **You rebased a feature branch you had already pushed, and now `git push` is rejected. What should you run, and why that option?**

    `git push --force-with-lease`. A force push is needed because the remote has the old commits. `--force-with-lease` is safer than `--force` because it won't overwrite work someone else pushed in the meantime.

10. **Name three things a data engineering repo's `.gitignore` should typically include. What can't `.gitignore` handle?**

    Any three of: `.env`/secrets files, virtual environments, data files, logs, `__pycache__`, notebook checkpoints. It can't remove notebook outputs, because they live inside the `.ipynb` file: clear them before committing or use `nbstripout`.

11. **What is the purpose of branch protection rules on `main`?**

    They keep `main` stable by requiring reviewed PRs and passing checks, and by blocking direct pushes and force pushes.

12. **List two characteristics of a pull request that is easy to review.**

    Any two of: small and focused scope, a clear description of what and why, testing notes, a linked ticket.
