-- them du lieu source table

delete from sourceconfig

INSERT INTO sourceconfig VALUES
('1', 'Thoi tiet VN', 'http://thoitiet.vn', 'weather_extract', 'weather_extract'),
('2', 'Thoi tiet edu VN', 'http://thoitiet.edu.vn', 'weather_extract', 'weather_extract'),
('3', 'Danh sach tinh VN', 'https://blog.rever.vn/danh-sach-63-tinh-thanh-viet-nam', 'weather_extract', 'weather_extract')

-- them du lieu ftp table
INSERT INTO ftpconfig VALUES
(1, '103.97.126.21', 21, 'ngsfihae', 'U05IIKw0HsICPNU', 1)

-- them du lieu dbconfig

INSERT INTO dbconfig VALUES
(1, 'jdbc:mysql://localhost:3306/stagging', 'root', null, 'stagging', 1),
(2, 'jdbc:mysql://localhost:3306/datawarehouseweather', 'root', null, 'datawarehouse', 1)