# Upload Sessions Service

## Session States

- `pending`: Upload session created
- `in_progress`: User actively uploads chunks
- `completed`: All chunks uploaded
- `failed`: Upload error occured
- `expired`: 2h timeout reached
- `cancelled`: Cancelled by user
