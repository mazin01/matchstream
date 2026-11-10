| scenario | rows (a × b) | matchstream | pandas merge | notes |
|---|---:|---:|---:|---|
| key match, normalize=False | 100,000 × 100,000 | 0.29s | 0.13s | same counts as pandas |
| key match, normalize=True | 100,000 × 100,000 | 0.28s | 0.13s | same counts as pandas |
| key match, normalize=False | 1,000,000 × 1,000,000 | 1.04s | 1.54s | same counts as pandas |
| key match, normalize=True | 1,000,000 × 1,000,000 | 1.05s | 1.54s | same counts as pandas |
| weight + date, no key | 100,000 × 100,000 | 0.32s | n/a | 100.0% paired; true partner for 100.0% of unique weights, 80.3% overall (20% are identical 99 kg pallets) |
