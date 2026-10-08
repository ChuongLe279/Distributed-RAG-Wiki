# Quy tắc làm việc nhóm

## 1. Quy tắc làm việc với Git

### Branch

- Khi bắt đầu viết code cho một công việc mới, tạo một branch mới.
- Sau khi branch được push và merge vào `develop`, xóa branch đó.
- Đặt tên branch theo mẫu:

  ```text
  feature/<tên-service>/<hành-động-service>
  ```

  Ví dụ: `feature/auth-service/create-login`.

### Commit

Đặt thông điệp commit theo mẫu:

```text
<type>(<scope>): <description>

[Body: Tại sao làm? Có ràng buộc gì đặc biệt không?]

[Footer: Liên kết mã Use Case / Ràng buộc nghiệp vụ / Breaking Change]
```

Chữ cái đầu của `description` viết thường. Ví dụ:

```text
feat(auth): add login rate limiting to prevent brute force

Implement Redis-based rate limiter: maximum 5 failed attempts per 15 mins.
```

**Các `type` được sử dụng:**

| Type | Ý nghĩa |
| --- | --- |
| `feat` | Tính năng mới, gồm cả event và endpoint mới. |
| `fix` | Sửa lỗi. |
| `refactor` | Sửa cấu hình hoặc cấu trúc code nhưng không thay đổi hành vi. |
| `test` | Chỉ sửa test. |
| `docs` | Chỉ sửa tài liệu, ví dụ: ADR, Specs. |
| `build` | Thay đổi dependency hoặc Dockerfile. |
| `chore` | Cấu hình hoặc bảo trì vặt, ví dụ: Kubernetes manifest, Docker Compose local. |


### Pull request (PR)

1. Nên giữ mỗi PR dưới 200 dòng.
2. Người tạo PR không tự merge PR vào nhánh `develop`.
3. Mỗi PR cần ít nhất một thành viên khác xung phong review.
4. Sau khi review xong, reviewer xác nhận trên nhóm để xem còn thành viên nào muốn review không.
5. Nếu không ai muốn review thêm, reviewer merge PR.
