| scenario | rows (a × b) | matchstream | pandas merge | notes |
|---|---:|---:|---:|---|
| key match, normalize=False | 100,000 × 100,000 | 0.52s | 0.22s | same counts as pandas |
| key match, normalize=True | 100,000 × 100,000 | 0.52s | 0.22s | same counts as pandas |
| key match, normalize=False | 1,000,000 × 1,000,000 | 2.90s | 4.97s | same counts as pandas |
| key match, normalize=True | 1,000,000 × 1,000,000 | 2.65s | 4.97s | same counts as pandas |
| amount + date, no key | 100,000 × 100,000 | 1.22s | n/a | 100.0% paired; true partner for 100.0% of unique amounts, 80.3% overall (20% are identical recurring amounts) |
