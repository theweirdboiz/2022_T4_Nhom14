package process;

import dao.warehouse.WarehouseDao;

public class ThirdProcessing {
	WarehouseDao warehouseDao;

	public ThirdProcessing() {
		warehouseDao = new WarehouseDao();
	}

	public boolean load() {
		boolean result = false;
//		load dim
		if (warehouseDao.loadDateDim() && warehouseDao.loadTimeDim() && warehouseDao.loadProvinceDim()) {
			if (!warehouseDao.getAllWeatherData()) {
				return warehouseDao.insertAllWeatherDataFromStaging();
			}
		}
		return result;

//		Lấy các dữ liệu đang sử dụng so sánh với dữ liệu mới trước khi load
	}

	public static void main(String[] args) {
		ThirdProcessing thirdProcessing = new ThirdProcessing();
		System.out.println(thirdProcessing.load());
	}
}
