create_dim_artist = '''
    CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
        id VARCHAR(30) PRIMARY KEY,
        name VARCHAR(50) 
    );
'''

create_dim_album = '''
    CREATE TABLE IF NOT EXSISTS {{ params.table_name }} (
        id VARCHAR(30) PRIMARY KEY,
        name VARCHAR(50),
        type VARCHAR(15),
        label VARCHAR(50),
        popularity INT,
        release_date DATE,
        copyrights VARCHAR(200),
        url VARCHAR(100),
        link_image VARCHAR(100)
    );
'''