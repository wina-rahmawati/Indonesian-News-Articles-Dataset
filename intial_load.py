'''Initial Load Script'''

import pandas as pd
import numpy as np

# Create dim_date
def create_dim_date(start_date, end_date):
    dates = pd.date_range(start=start_date, end=end_date)
    dim_date = pd.DataFrame(dates, columns=['date'])
    dim_date['date_key'] = dim_date['date'].dt.strftime('%Y%m%d')
    dim_date['day'] = dim_date['date'].dt.day
    dim_date['month'] = dim_date['date'].dt.month
    dim_date['quarter'] = dim_date['date'].dt.quarter
    dim_date['year'] = dim_date['date'].dt.year
    dim_date['week_of_year'] = dim_date['date'].dt.isocalendar().week
    dim_date['day_of_week'] = dim_date['date'].dt.dayofweek + 1 
    dim_date['is_weekend'] = np.where(dim_date['day_of_week'].isin([6, 7]), True, False)
    dim_date.set_index('date_key', inplace=True)
    dim_date.to_csv('data/result_dim_date.csv', sep=';', index=False)


# Create dim_author
def create_dim_author():
    dim_author = {
        'author_id': [1,2,3,4,5,6,7],
        'author_name': ['Yoon Jeonghan', 'Hong Jisoo', 'Lee Jihoon', 'Lee Seokmin', 'Boo Seungkwan', 'Kim Mingyu', 'Lee Chan']
    }
    dim_author = pd.DataFrame(dim_author)
    dim_author.to_csv('data/result_dim_author.csv', sep=';', index=False)

# Create dim_article_catgeory
def create_dim_article_category():
    article_category = {
        'id': [1,2,3,4,5,6,7,8,9,10,11],
        'category_name': ['religion', 'education', 'finance', 'food', 'health', 'automotive', 'sport', 'politics', 'criminal', 'Entertainment', 'Other']
    }
    article_category = pd.DataFrame(article_category)
    article_category.to_csv('data/result_dim_article_category.csv', sep=';', index=False)

def categorize_article(title):
    categories = {
    1: ['agama', 'kepercayaan', 'gereja', 'masjid', 'kuil', 'iman', 'doa', 'kitab suci', 'spiritual', 'ritual', 'puasa', 'peribadatan', 'teologi', 'penganut', 'ajaran', 'jamaah', 'haji'],
    2: ['pendidikan', 'sekolah', 'universitas', 'kampus', 'pelajaran', 'kursus', 'guru', 'siswa', 'kelas', 'studi', 'akademik', 'pelatihan', 'ujian', 'pengetahuan', 'belajar', 'kurikulum'],
    3: ['keuangan', 'saham', 'pasar', 'ekonomi', 'investasi', 'perbankan', 'perdagangan', 'aset', 'portofolio', 'bunga', 'keuntungan', 'kerugian', 'obligasi', 'sekuritas', 'modal'],
    4: ['resep', 'makanan', 'masakan', 'restoran', 'memasak', 'hidangan', 'bahan', 'bakery', 'koki', 'diet', 'gourmet', 'nutrisi', 'camilan', 'minuman'],
    5: ['kesehatan', 'kebugaran', 'wellness', 'diet', 'olahraga', 'obat', 'perawatan', 'nutrisi', 'mental', 'penyakit', 'pencegahan', 'pengobatan', 'gejala', 'yoga', 'latihan'],
    6: ['otomotif', 'mobil', 'kendaraan', 'mesin', 'berkendara', 'motor', 'perbaikan', 'perawatan', 'teknologi', 'model', 'bahan bakar', 'baterai', 'transmisi', 'sedan', 'suv'],
    7: ['olahraga', 'tim', 'pertandingan', 'kejuaraan', 'kemenangan', 'pertandingan', 'atlet', 'pelatihan', 'kompetisi', 'skor', 'gol', 'pemain', 'turnamen', 'pelatih', 'liga', 'olimpiade','jakmania'],
    8: ['politik', 'pemerintahan', 'kebijakan', 'pemilihan', 'hukum', 'partai', 'calon', 'suara', 'demokrasi', 'legislasi', 'kongres', 'senat', 'presiden', 'debat', 'reformasi', 'jokowi', 'mpr', 'dpr', 'bupati', 'walikota', 'gubernur'],
    9: ['kriminal', 'kejahatan', 'pencurian', 'pembunuhan', 'polisi', 'penangkapan', 'penyidikan', 'persidangan', 'pengadilan', 'keadilan', 'tersangka', 'pelanggaran', 'penipuan', 'polri', 'terdakwa', 'ham', 'hakim'],   
    10: ['hiburan', 'film', 'movie', 'musik', 'konser', 'acara', 'televisi', 'teater', 'drama', 'komedi', 'bintang', 'celebrity', 'show', 'video', 'game', 'permainan', 'pertunjukan']}
    title_lower = title.lower()
    for category, keywords in categories.items():
        if any(keyword in title_lower for keyword in keywords):
            return category
    return 11

# Main
def main():
    path = 'data'
    dim_date_data = create_dim_date('2018-01-01', '2024-12-31')
    dim_author_data = create_dim_author()
    dim_dim_article_category_data = create_dim_article_category()

    df = pd.read_csv(f'{path}/Indonesian News Dataset.csv')
    df = df[['id', 'title', 'content', 'date', 'created_at', 'updated_at']]
    df = df.rename(columns={'created_at': 'published_at', 'date':'created_at'})
    df['author_id'] = np.random.randint(1, 8, df.shape[0]) # Assuming for author_id
    df.to_csv(f'{path}/result_data_preparation.csv', sep=';', index=False)

    df['article_category_id'] = df['title'].apply(categorize_article)
    df['updated_at'] = pd.to_datetime(df['updated_at'], format="%Y-%m-%d")
    df['updated_at_id'] = df.updated_at.dt.strftime('%Y%m%d')
    df.to_csv(f'{path}/result_initial_load.csv', sep=';', index=False)

a = main()