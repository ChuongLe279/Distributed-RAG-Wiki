from datasets import load_dataset

# Tải bản snapshot tiếng Anh (ví dụ mốc tháng 11/2025)
ds = load_dataset("wikimedia/wikipedia", "20231101.en", split="train")
# Ghi ra Parquet để giữ chặt schema và tối ưu I/O
ds.to_parquet("en_wikipedia_20251101.parquet") # type: ignore