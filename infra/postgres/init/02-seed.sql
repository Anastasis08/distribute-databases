INSERT INTO products (sku, name, category, price, quantity) VALUES
    ('KB-K2-RGB',    'Механічна клавіатура Keychron K2',  'Периферія', 3899.00, 12),
    ('MS-MX3S',      'Миша Logitech MX Master 3S',        'Периферія', 4299.00,  8),
    ('MN-U2724D',    'Монітор Dell UltraSharp U2724D',    'Монітори', 17999.00,  4),
    ('HP-WH1000XM5', 'Навушники Sony WH-1000XM5',         'Аудіо',    13499.00,  6),
    ('NB-XPS13',     'Ноутбук Dell XPS 13',               'Ноутбуки', 54999.00,  3),
    ('SSD-990P-1T',  'SSD Samsung 990 PRO 1TB',           'Накопичувачі', 4199.00, 20)
ON CONFLICT (sku) DO NOTHING;
