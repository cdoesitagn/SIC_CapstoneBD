# Import thư viện
import pymongo
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
from scipy.signal import savgol_filter

# Kết nối với cơ sở dữ liệu để lấy thông tin về giá
c = MongoClient("100.89.103.30:27017", 27017)
db = c["stockDB"]
industry = db.industries
eodPrices = db.eodPrices

# Tìm ra tất cả thông tin về giá cảu ngành ngân hàng
nganhangID = industry.find_one({"name": "Ngân hàng"}, {"_id": 1})["_id"]
nganhang_eodPrices = eodPrices.find({"industryId": nganhangID})
nganhang_df = pd.DataFrame(nganhang_eodPrices)
nganhang_df['date'] = pd.to_datetime(nganhang_df['date']).dt.strftime("%Y-%m-%d")
nganhang_df = nganhang_df[["date", "close", "open", "high", "low", "change", "percent_change"]].groupby('date').mean().reset_index()

# Lựa chọn giá đóng cửa cuối ngày để xây dựng dataset, sử dụng Savitzky Golay để làm mượt dữ liệu
nganhang_df["smooth_close"] = savgol_filter(nganhang_df["close"], 25, 3)
nganhang_df = nganhang_df[["date", "close", "smooth_close"]]

# Lẩy ra các đặc trưng về sự tăng giảm giá ngắn hạn (1 ngày), trung hạn (7 ngày) và dài hạn (30 ngày)
nganhang_df["Near"] = 0.0
nganhang_df["Mid"] = 0.0
nganhang_df["Far"] = 0.0
for i in range(len(nganhang_df)-30):
    nganhang_df.loc[i, "Near"] = 0 if nganhang_df.loc[i+1, "smooth_close"] - nganhang_df.loc[i, "smooth_close"] < 0 else 1
    nganhang_df.loc[i, "Mid"] = 0 if nganhang_df.loc[i+7, "smooth_close"] - nganhang_df.loc[i, "smooth_close"] < 0 else 1
    nganhang_df.loc[i, "Far"] = 0 if nganhang_df.loc[i+30, "smooth_close"] - nganhang_df.loc[i, "smooth_close"] < 0 else 1
nganhang_df.rename(columns={"date": "Date"}, inplace=True)

# Đọc tập dữ liệu các bài báo đã kéo được từ trang CafeF https://cafef.vn/
articles_data = pd.read_csv("../../data/raw/tai_chinh_ngan_hang.csv")[["Published_Date", "Title", "Text"]].dropna()
articles_data["Published_Date"] = pd.to_datetime(articles_data["Published_Date"]).dt.strftime("%Y-%m-%d")
articles_data.rename(columns={"Published_Date": "Date"}, inplace=True)
articles_data.sort_values(by = 'Date', inplace=True)
articles_data.reset_index(inplace = True)
articles_data = articles_data.drop("index", axis=1)

# Hợp nhất tất cả tiêu đề và nội dung bài báo của cùng một ngày
title_data = articles_data.groupby('Date')["Title"].apply(', '.join).reset_index()
document_data = articles_data.groupby('Date')["Text"].apply(', '.join).reset_index()

# Gán các nhãn đặc trưng về giá Near, Mid, High với thông tin các bài báo theo ngày
labeled = pd.merge(pd.concat((title_data, document_data['Text']), axis=1), 
                    nganhang_df[["Date", "Near", "Mid", "Far"]], on='Date', how='left').dropna()
# Lấy dữ liệu đến 2024-07-01
labeled = labeled[labeled['Date'] <= "2024-07-01"]

# Tính toán chỉ số Score của mỗi ngày theo các đặc trưng ngắn hạn, trung hạn, dài hạn với trọng số tăng dần
labeled['Score'] = labeled['Near'] + labeled['Mid'] * 2 + labeled['Far'] * 3

# Tính toán ngày đó thông tin có tiềm năng hay không sử dụng Score
labeled['Potential'] = None
for i in range (len(labeled)):
    if labeled.iloc[i,-2] < 2:
        labeled.iloc[i,-1] = 'Low'
    else:
        labeled.iloc[i,-1] = 'High'

# Lưu tập dữ liệu
labeled.to_csv("../../data/processed/labeled_data.csv")


