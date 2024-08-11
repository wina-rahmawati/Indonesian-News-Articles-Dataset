import pandas as pd

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
    df = pd.read_csv(f'/opt/airflow/modules/MIRRORING_TEMP/article_temp.csv')
    df['article_category_id'] = df['title'].apply(categorize_article)

    df['updated_at'] = pd.to_datetime(df['updated_at'], format="%Y-%m-%d")
    df['updated_at_id'] = df.updated_at.dt.strftime('%Y%m%d')
    df.to_csv('article_temp.csv', sep=';', index=False)