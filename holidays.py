# holidays.py

import pandas as pd

def generate_range_holiday(name: str, start_date: str, end_date: str) -> pd.DataFrame:
    dates = pd.date_range(start=start_date, end=end_date)
    return pd.DataFrame({
        'holiday': [name] * len(dates),
        'ds': dates,
        'lower_window': [0] * len(dates),
        'upper_window': [0] * len(dates)
    })

black_friday_2023 = generate_range_holiday('black_friday_2023', '2023-11-24', '2023-11-24')
black_friday_2024 = generate_range_holiday('black_friday_2024', '2024-11-29', '2024-11-29')

new_year_2023 = generate_range_holiday('new_year_2023', '2023-12-23', '2023-12-31')
new_year_2024 = generate_range_holiday('new_year_2024', '2024-12-23', '2024-12-31')

fixed_days = pd.DataFrame({
    'holiday': ['plus_birthday', '8_march', '8_march', 'plus_birthday'],
    'ds': pd.to_datetime(['2024-07-05', '2024-03-08', '2025-03-08', '2025-07-05']),
    'lower_window': [0, 0, 0, 0],
    'upper_window': [0, 0, 0, 0]
})

holidays = pd.concat([
    black_friday_2023,
    black_friday_2024,
    new_year_2023,
    new_year_2024,
    fixed_days
], ignore_index=True)