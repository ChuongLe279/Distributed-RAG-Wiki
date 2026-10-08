## Repository

| Kiểu dữ liệu | Các trường |
| --- | --- |
| `integer` | `id`, `size`, `stargazers_count`, `forks_count`, `watchers_count`, `open_issues_count` |
| `string` | `name`, `full_name`, `default_branch` |
| `string \| null` | `description`, `language` |
| `string (datetime) \| null` | `created_at`, `updated_at`, `pushed_at` |
| `array<string>` | `topics` |
| `object` | `owner` |
| `object \| null` | `license` |
| `boolean` | `archived`, `fork` |

## Issue

| Kiểu dữ liệu | Các trường |
| --- | --- |
| `integer` | `id`, `number`, `comments` |
| `string` | `title`, `state` |
| `string \| null` | `body`, `state_reason` |
| `object` | `user` |
| `object \| null` | `milestone`, `closed_by` |
| `array<object>` | `labels`, `assignees` |
| `string (datetime)` | `created_at`, `updated_at` |
| `string (datetime) \| null` | `closed_at` |

## Pull request

| Kiểu dữ liệu | Các trường |
| --- | --- |
| `integer` | `id`, `number`, `commits`, `additions`, `deletions`, `changed_files` |
| `string` | `title`, `state` |
| `string \| null` | `body` |
| `boolean` | `draft` |
| `boolean \| null` | `mergeable` |
| `object` | `user`, `head`, `base` |
| `object \| null` | `merged_by` |
| `string (datetime)` | `created_at`, `updated_at` |
| `string (datetime) \| null` | `closed_at`, `merged_at` |

## Commit

| Kiểu dữ liệu | Các trường |
| --- | --- |
| `string` | `sha`, `commit.message` |
| `object` | `commit.author`, `commit.committer` |
| `object \| null` | `author`, `committer` |
| `array<object>` | `parents`, `files` |
| `integer` | `stats.additions`, `stats.deletions`, `stats.total` |

## Issue/PR comment

| Kiểu dữ liệu | Các trường |
| --- | --- |
| `integer` | `id` |
| `string` | `body`, `author_association` |
| `object` | `user` |
| `string (datetime)` | `created_at`, `updated_at` |

## User

| Kiểu dữ liệu | Các trường |
| --- | --- |
| `integer` | `id`, `public_repos`, `followers`, `following` |
| `string` | `login`, `type` |
| `string \| null` | `name`, `company`, `location`, `bio` |
| `string (datetime)` | `created_at` |
