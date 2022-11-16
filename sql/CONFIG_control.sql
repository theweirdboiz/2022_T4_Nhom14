INSERT INTO sourceconfig VALUES
(1, '63 tỉnh thành','https://vi.wikipedia.org/wiki/T%E1%BB%89nh_th%C3%A0nh_Vi%E1%BB%87t_Nam'),
(2, 'Thời tiết VN','https://thoitiet.vn/'),
(3, 'Thời tiết Edu VN','https://thoitiet.vn/')

INSERT INTO dbconfig VALUES 
('1','localhost:3306/staging','staging','root',null,1),
('2','localhost:3306/datawarehouse','datawarehouse','root',null,1)

INSERT INTO ftpconfig VALUES ('1','103.97.126.21',21,'ngsfihae','U05IIKw0HsICPNU',1);