

import unittest
from unittest.mock import MagicMock  


from utils.deduplicator import Deduplicator

class TestDeduplicator(unittest.TestCase):

    def setUp(self):
        """Hàm này sẽ chạy trước mỗi test case, dùng để setup môi trường test."""
        self.mock_db_client = MagicMock()
        
        self.deduplicator = Deduplicator(self.mock_db_client)

    def test_filter_new_listings_with_mixed_data(self):
        """Test case 1: Lọc thành công với cả tin mới và tin cũ."""
        print("Running: test_filter_new_listings_with_mixed_data")
        
        scraped_listings = [
            {'source_id': 'id1', 'title': 'Nhà mới 1'},
            {'source_id': 'id2', 'title': 'Nhà cũ 2'},
            {'source_id': 'id3', 'title': 'Nhà mới 3'},
        ]
        
        self.mock_db_client.get_existing_ids.return_value = {'id2'}
        
        new_listings = self.deduplicator.filter_new_listings('batdongsan', scraped_listings)
        
        self.assertEqual(len(new_listings), 2)
        
        new_ids = {listing['source_id'] for listing in new_listings}
        self.assertEqual(new_ids, {'id1', 'id3'})

    def test_filter_with_all_new_listings(self):
        """Test case 2: Tất cả đều là tin mới."""
        print("Running: test_filter_with_all_new_listings")
        scraped_listings = [
            {'source_id': 'id1', 'title': 'Nhà mới 1'},
            {'source_id': 'id2', 'title': 'Nhà mới 2'},
        ]
        
        self.mock_db_client.get_existing_ids.return_value = set()
        
        new_listings = self.deduplicator.filter_new_listings('batdongsan', scraped_listings)
        
        self.assertEqual(len(new_listings), 2)
        self.assertEqual(new_listings, scraped_listings)

    def test_filter_with_all_existing_listings(self):
        """Test case 3: Tất cả đều là tin cũ."""
        print("Running: test_filter_with_all_existing_listings")
        scraped_listings = [
            {'source_id': 'id1', 'title': 'Nhà cũ 1'},
            {'source_id': 'id2', 'title': 'Nhà cũ 2'},
        ]
        
        self.mock_db_client.get_existing_ids.return_value = {'id1', 'id2'}
        
        new_listings = self.deduplicator.filter_new_listings('batdongsan', scraped_listings)
        
        self.assertEqual(len(new_listings), 0)

    def test_filter_with_empty_input_list(self):
        """Test case 4: Đầu vào là danh sách rỗng."""
        print("Running: test_filter_with_empty_input_list")
        
        new_listings = self.deduplicator.filter_new_listings('batdongsan', [])
        
        self.assertEqual(len(new_listings), 0)
        self.assertEqual(new_listings, [])
        
        self.mock_db_client.get_existing_ids.assert_not_called()

    def test_filter_with_missing_source_id(self):
        """Test case 5: Tin đăng thiếu source_id."""
        print("Running: test_filter_with_missing_source_id")
        scraped_listings = [
            {'source_id': 'id1', 'title': 'Nhà có ID'},
            {'title': 'Nhà không có ID'}, 
            {'source_id': 'id3', 'title': 'Nhà khác có ID'},
        ]
        
        self.mock_db_client.get_existing_ids.return_value = set()
        
        new_listings = self.deduplicator.filter_new_listings('batdongsan', scraped_listings)
        
        self.assertEqual(len(new_listings), 2)
        new_ids = {listing['source_id'] for listing in new_listings}
        self.assertEqual(new_ids, {'id1', 'id3'})


if __name__ == '__main__':
    unittest.main()