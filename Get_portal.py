# Import necessary modules
from seleniumwire import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import psutil
import time

def getToken(email, password):
    # Set up Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--proxy-server='direct://'")
    chrome_options.add_argument("--proxy-bypass-list=*")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--allow-running-insecure-content")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--disable-infobars")

    # Initialize the WebDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    def login(driver, email, password):
        # Open the URL
        driver.get("https://portal.taxi.booking.com/")
        print("Opened URL")

        # Wait for the email input to be present
        email_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='email'][name='email']"))
        )
        print("Email input found")

        # Find the email and password input fields
        email_input = driver.find_element(By.CSS_SELECTOR, "input[type='email'][name='email']")
        password_input = driver.find_element(By.CSS_SELECTOR, "input[type='password'][name='password']")

        # Fill in the login details
        email_input.send_keys(email)
        password_input.send_keys(password)
        print("Filled in login details")

        # Find and click the sign-in button
        sign_in_button = driver.find_element(By.CSS_SELECTOR, "button[data-testid='button--login-submit']")
        sign_in_button.click()
        print("Clicked sign-in button")

    def waitForRedirect(driver, url):
        WebDriverWait(driver, 30).until(EC.url_to_be(url))
        print(f"Redirected to {url}")

    def captureAuthHeader(driver, timeout=30):
        # Wait for the specific network request and capture the authorization header
        end_time = time.time() + timeout
        while time.time() < end_time:
            for request in driver.requests:
                auth_header = request.headers.get('Authorization')
                if auth_header and auth_header.startswith('Bearer'):
                    print(f"Request URL: {request.url}")
                    print(f"Authorization header: {auth_header}")
                    return auth_header
            time.sleep(1)
        raise Exception("Authorization header not found within the timeout period")

    try:
        # Perform login
        login(driver, email, password)

        # Wait for the page to redirect to the desired URL
        waitForRedirect(driver, "https://portal.taxi.booking.com/bookings/rides")

        # Capture the authorization header
        bearer_token = captureAuthHeader(driver)
        return bearer_token
    finally:
        # Close the browser
        driver.quit()

        # Ensure all Chrome processes are terminated
        for proc in psutil.process_iter(['pid', 'name']):
            if proc.info['name'] in ("chrome", "chromedriver"):
                psutil.Process(proc.info['pid']).terminate()
                try:
                    psutil.Process(proc.info['pid']).wait(timeout=3)
                except psutil.TimeoutExpired:
                    psutil.Process(proc.info['pid']).kill()


if __name__ == "__main__":
    email = "anhtana3hlk@gmail.com"
    password = "Tmai091092@@"
    token = getToken(email, password)
    print(f"Token: {token}")
    
    
# ============================================================================================================================================================================
import requests
import pandas as pd
pd.set_option('display.max_columns', None)

# URLs and token
base_url = "https://portal.taxi.booking.com/api/"
token = token  # Your authorization token

# Function to make requests
def get_request(url, headers=None, params=None):
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        return response.json()  # Return JSON content if the request was successful
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None

# Fetch preferred status data
def fetch_preferred_status(start_date, end_date):
    preferred_status_url = base_url + "supplier/performance"
    headers = {"Authorization": token}
    params = {"startDate": start_date, "endDate": end_date}
    response_data = get_request(preferred_status_url, headers=headers, params=params)
    if response_data:
        print("Preferred status data fetched successfully.")
        return response_data
    else:
        print("Failed to fetch preferred status data.")
        return None

# Fetch ride data
def fetch_ride_data(date_from, date_to, page_size=500, max_pages=500):
    url = base_url + "reports/rides"
    headers = {"Authorization": token}
    params = {
        "pageSize": str(page_size),
        "dateFrom": date_from,
        "dateTo": date_to,
        "supplierLocationIds": "",
        "hasRideRating": "false",
        "hasIncident": "false"
    }
    combined_data = []
    for page in range(1, max_pages + 1):
        params["pageNumber"] = str(page)
        response_data = get_request(url, headers=headers, params=params)
        if response_data and 'results' in response_data and response_data['results']:
            combined_data.extend(response_data['results'])
        else:
            print(f"No data found on page {page}. Exiting loop.")
            break
    print("Total number of records:", len(combined_data))
    return combined_data

# Fetch location data (bookingv2)
def fetch_location_data(pickup_date_from, pickup_date_to):
    location_url = base_url + f"bookings/v2?meta=true&pickUpDateFrom={pickup_date_from}&pickUpDateTo={pickup_date_to}"
    headers = {"Authorization": token}
    response_data = get_request(location_url, headers=headers)
    if response_data:
        print("Location data fetched successfully.")
        return response_data
    else:
        print("Failed to fetch location data.")
        return None

# Parameters
start_date = "2024-06-01"
end_date = "2024-06-31"
date_from = "2024-06-01T00:00:00"
date_to = "2024-06-01T23:59:59"
pickup_date_from = "2024-06-09T17:00"
pickup_date_to = "2024-06-10T16:59"

# Fetch data
preferred_status = fetch_preferred_status(start_date, end_date)
ride_data = fetch_ride_data(date_from, date_to)
#location_data_bookingv2 = fetch_location_data(pickup_date_from, pickup_date_to)

# ============================================================================================================================================================================
import pandas as pd

# Hàm xử lý sự kiện driver
def process_driver_events(driver_events):
    departed_to_pickup = None
    arrived_at_pickup = None
    departed_to_dropoff = None
    arrived_at_dropoff = None

    latitude_departed_to_pickup = None
    longitude_departed_to_pickup = None
    latitude_arrived_at_pickup = None
    longitude_arrived_at_pickup = None
    latitude_departed_to_dropoff = None
    longitude_departed_to_dropoff = None
    latitude_arrived_at_dropoff = None
    longitude_arrived_at_dropoff = None

    error_departed_to_pickup = None
    error_arrived_at_pickup = None
    error_departed_to_dropoff = None
    error_arrived_at_dropoff = None

    for event in driver_events:
        event_type = event.get("type")
        timestamp = event.get("timestamp")
        coordinates = event.get("coordinates")
        error = event.get("error")

        if event_type == "DRIVER_DEPARTED_TO_PICKUP" and timestamp is not None:
            departed_to_pickup = timestamp
            if coordinates:
                latitude_departed_to_pickup = coordinates.get("latitude")
                longitude_departed_to_pickup = coordinates.get("longitude")
            error_departed_to_pickup = error
        elif event_type == "DRIVER_ARRIVED_AT_PICKUP" and timestamp is not None:
            arrived_at_pickup = timestamp
            if coordinates:
                latitude_arrived_at_pickup = coordinates.get("latitude")
                longitude_arrived_at_pickup = coordinates.get("longitude")
            error_arrived_at_pickup = error
        elif event_type == "DRIVER_DEPARTED_TO_DROPOFF" and timestamp is not None:
            departed_to_dropoff = timestamp
            if coordinates:
                latitude_departed_to_dropoff = coordinates.get("latitude")
                longitude_departed_to_dropoff = coordinates.get("longitude")
            error_departed_to_dropoff = error
        elif event_type == "DRIVER_ARRIVED_AT_DROPOFF" and timestamp is not None:
            arrived_at_dropoff = timestamp
            if coordinates:
                latitude_arrived_at_dropoff = coordinates.get("latitude")
                longitude_arrived_at_dropoff = coordinates.get("longitude")
            error_arrived_at_dropoff = error

    return (
        departed_to_pickup, arrived_at_pickup, departed_to_dropoff, arrived_at_dropoff,
        latitude_departed_to_pickup, longitude_departed_to_pickup,
        latitude_arrived_at_pickup, longitude_arrived_at_pickup,
        latitude_departed_to_dropoff, longitude_departed_to_dropoff,
        latitude_arrived_at_dropoff, longitude_arrived_at_dropoff,
        error_departed_to_pickup, error_arrived_at_pickup,
        error_departed_to_dropoff, error_arrived_at_dropoff
    )
# ============================================================================================================================================================================
# Trích xuất thông tin cần thiết từ dữ liệu JSON
bookings = ride_data
booking_info_list = []

for booking in bookings:
    booking_id = booking["bookingId"]
    rideStatus = booking["rideStatus"]
    pickupDateTimeUTC = booking["pickupDateTimeUTC"]
    pickupDateTimeLocal = booking["pickupDateTimeLocal"]
    driver_name = booking.get('driverName', 'NA')
    
    driverEventsStatus = booking.get('driverEventsStatus','NA')
    fixed_status = driverEventsStatus[0].strip("[]")
    
    price_raw = booking['rate']['amount']
    fixed_price = float(str(price_raw).strip(','))
    
    currency = booking['rate']['currency']
    
    drivingDistanceInKm = booking.get('drivingDistanceInKm','NA')
    
    pickupLocation = booking["pickupLocation"]
    
    dropoffLocation = booking["dropoffLocation"]
    
    timezone = booking["timezone"]
    
    ridesReview = 1 if booking["ridesReview"] else 0
    comments = booking["ridesReview"]["comments"][0] if (ridesReview and "comments" in booking["ridesReview"] and booking["ridesReview"]["comments"]) else "NA"
    rideScore = booking["ridesReview"]["rideScore"] if (ridesReview and "rideScore" in booking["ridesReview"]) else "NA"

    supplierLocationName = booking["supplierLocationName"]
   
    incidentStatus = booking.get('incidentStatus','NA')
    
    incidentType = booking.get("incidentType", "NA")
    
    driver_events = booking["driverEvents"]
    
    # Lấy các giá trị timestamp và các thông tin liên quan từ các sự kiện driver
    (
        departed_to_pickup, arrived_at_pickup, departed_to_dropoff, arrived_at_dropoff,
        latitude_departed_to_pickup, longitude_departed_to_pickup,
        latitude_arrived_at_pickup, longitude_arrived_at_pickup,
        latitude_departed_to_dropoff, longitude_departed_to_dropoff,
        latitude_arrived_at_dropoff, longitude_arrived_at_dropoff,
        error_departed_to_pickup, error_arrived_at_pickup,
        error_departed_to_dropoff, error_arrived_at_dropoff
    ) = process_driver_events(driver_events)
    
    booking_info = {
        "Booking ID": booking_id,
        "supplierLocationName":supplierLocationName,
        "rideStatus": rideStatus,
        "incidentStatus": incidentStatus,
        "incidentType": incidentType,
        "pickup Date Time UTC": pickupDateTimeUTC,
        "pickupDateTimeLocal": pickupDateTimeLocal,
        "Driver's Name": driver_name,
        "driverEventsStatus": fixed_status,
        "price": fixed_price,
        "currency": currency,
        "drivingDistanceInKm": drivingDistanceInKm,
        "pickupLocation": pickupLocation,
        "dropoffLocation": dropoffLocation,
        "timezone": timezone,
        "ridesReview": ridesReview,
        "comments": comments,
        "rideScore": rideScore,
        "DRIVER_DEPARTED_TO_PICKUP": departed_to_pickup,
        "DRIVER_ARRIVED_AT_PICKUP": arrived_at_pickup,
        "DRIVER_DEPARTED_TO_DROPOFF": departed_to_dropoff,
        "DRIVER_ARRIVED_AT_DROPOFF": arrived_at_dropoff,
        "latitude_departed_to_pickup": latitude_departed_to_pickup,
        "longitude_departed_to_pickup": longitude_departed_to_pickup,
        "latitude_arrived_at_pickup": latitude_arrived_at_pickup,
        "longitude_arrived_at_pickup": longitude_arrived_at_pickup,
        "latitude_departed_to_dropoff": latitude_departed_to_dropoff,
        "longitude_departed_to_dropoff": longitude_departed_to_dropoff,
        "latitude_arrived_at_dropoff": latitude_arrived_at_dropoff,
        "longitude_arrived_at_dropoff": longitude_arrived_at_dropoff,
        "error_departed_to_pickup": error_departed_to_pickup,
        "error_arrived_at_pickup": error_arrived_at_pickup,
        "error_departed_to_dropoff": error_departed_to_dropoff,
        "error_arrived_at_dropoff": error_arrived_at_dropoff
    }
    booking_info_list.append(booking_info)

# Tạo DataFrame từ danh sách thông tin booking
df = pd.DataFrame(booking_info_list)
# ============================================================================================================================================================================


# Tính toán cột Count dựa trên giá trị của các cột error
def calculate_error_percentage(row):
    error_departed_to_pickup = row['error_departed_to_pickup']
    error_arrived_at_pickup = row['error_arrived_at_pickup']
    error_departed_to_dropoff = row['error_departed_to_dropoff']
    error_arrived_at_dropoff = row['error_arrived_at_dropoff']

    error_count = 0
    
    # Kiểm tra và đếm số lượng giá trị False
    if error_departed_to_pickup is False:
        error_count += 1
    if error_arrived_at_pickup is False:
        error_count += 1
    if error_departed_to_dropoff is False:
        error_count += 1
    if error_arrived_at_dropoff is False:
        error_count += 1

    # Tính toán phần trăm dựa trên số lượng giá trị False
    if error_count == 4:
        return 1.0  # 100%
    elif error_count == 3:
        return 0.75  # 75%
    elif error_count == 2:
        return 0.5  # 50%
    elif error_count == 1:
        return 0.25  # 25%
    else:
        return 0.0  # 0%

# Áp dụng hàm tính toán cho DataFrame
df['Error Percentage'] = df.apply(calculate_error_percentage, axis=1)

# ============================================================================================================================================================================
Dim_driver_event = df[['Booking ID', 
                       'DRIVER_DEPARTED_TO_PICKUP',
                       'latitude_departed_to_pickup',
                       'longitude_departed_to_pickup',
                       'DRIVER_ARRIVED_AT_PICKUP',
                       'latitude_arrived_at_pickup',
                       'longitude_arrived_at_pickup',
                       'DRIVER_DEPARTED_TO_DROPOFF',
                       'latitude_departed_to_dropoff',
                       'longitude_departed_to_dropoff',
                       'DRIVER_ARRIVED_AT_DROPOFF', 
                       'latitude_arrived_at_dropoff',
                       'longitude_arrived_at_dropoff',
                       'error_departed_to_pickup',
                       'error_arrived_at_pickup',
                       'error_departed_to_dropoff',
                       'error_arrived_at_dropoff',
                       'Error Percentage' 
                       ]]
df_driver_performance_by_ID = df.drop(['latitude_departed_to_pickup',
         'longitude_departed_to_pickup',
         'latitude_arrived_at_pickup',
         'longitude_arrived_at_pickup',
         'latitude_departed_to_dropoff',
         'longitude_departed_to_dropoff',
         'latitude_arrived_at_dropoff',
         'longitude_arrived_at_dropoff',
         'error_departed_to_pickup',
         'error_arrived_at_pickup',
         'error_departed_to_dropoff',
         'error_arrived_at_dropoff',
         'Error Percentage' ],axis=1)
# Export to excel Dim_driver_event  

Dim_driver_event.to_excel("Dim Driver Event Daily.xlsx", index = False)
df_driver_performance_by_ID.to_excel("Driver Performance VPS automation.xlsx", index = False)

# ============================================================================================================================================================================

# ============================================================================================================================================================================

# ============================================================================================================================================================================
#Overwrite lên file Dim Driver Event Daily (SharePoint)
import requests

# Các thông tin cần thiết
tenant_id = 'a3f88450-77ef-4df3-89ea-c69cbc9bc410'
client_id = 'ad6b066a-d749-4f0b-bfbb-bad8de0af5d1'
client_secret = 'YwZ8Q~N6dAwc~sTcMAQsDQXwCKDfPBk81miLVbL4'
site_id = 'fbdd4069-e12d-4a30-b316-926cebd4972e'
drive_id = 'b!aUDd-y3hMEqzFpJs69SXLqsFboc6d3VHuXjQmhhH2yyWDIb9eEGSTJmyXz-tN3EO'

append_url = f'https://graph.microsoft.com/v1.0/sites/fbdd4069-e12d-4a30-b316-926cebd4972e/lists/fd860c96-4178-4c92-99b2-5f3fad37710e/items/15/driveitem/workbook/worksheets/Sheet1/tables/Table1/rows/add'

# Endpoint để overwrite file
update_url = f"https://graph.microsoft.com/v1.0/sites/fbdd4069-e12d-4a30-b316-926cebd4972e/drives/b!aUDd-y3hMEqzFpJs69SXLqsFboc6d3VHuXjQmhhH2yyWDIb9eEGSTJmyXz-tN3EO/root:/Driver Performance VPS automation.xlsx:/content"

# Access token
token_url = f'https://login.microsoftonline.com/a3f88450-77ef-4df3-89ea-c69cbc9bc410/oauth2/v2.0/token'
token_data = {
    'grant_type': 'client_credentials',
    'client_id': 'ad6b066a-d749-4f0b-bfbb-bad8de0af5d1',
    'client_secret': 'YwZ8Q~N6dAwc~sTcMAQsDQXwCKDfPBk81miLVbL4',
    'scope': 'https://graph.microsoft.com/.default'
}

token_r = requests.post(token_url, data=token_data)
access_token = token_r.json()['access_token']

# Headers
headers = {
    'Authorization': 'Bearer ' + access_token,
    'Content-Type': 'application/x-www-form-urlencoded',
}

# Đọc dữ liệu file và gửi lên SharePoint
# Thay đổi, cung  cấp dường dẫn của file đã lưu để mở file
with open('Driver Performance VPS automation.xlsx', 'rb') as file:
    file_content = file.read()
    response = requests.put(update_url, headers=headers, data=file_content)

if response.status_code == 200:
    print("ghi đè thông tin river Performance thành công!")
else:
    print("Có lỗi xảy ra khi ghi đè thông tin.")
    
# ============================================================================================================================================================================

# ============================================================================================================================================================================

# ============================================================================================================================================================================

#Overwrite lên file Dim Driver Event Daily (SharePoint)
import requests

# Các thông tin cần thiết
tenant_id = 'a3f88450-77ef-4df3-89ea-c69cbc9bc410'
client_id = 'ad6b066a-d749-4f0b-bfbb-bad8de0af5d1'
client_secret = 'YwZ8Q~N6dAwc~sTcMAQsDQXwCKDfPBk81miLVbL4'
site_id = 'fbdd4069-e12d-4a30-b316-926cebd4972e'
drive_id = 'b!aUDd-y3hMEqzFpJs69SXLqsFboc6d3VHuXjQmhhH2yyWDIb9eEGSTJmyXz-tN3EO'

append_url = f'https://graph.microsoft.com/v1.0/sites/fbdd4069-e12d-4a30-b316-926cebd4972e/lists/fd860c96-4178-4c92-99b2-5f3fad37710e/items/15/driveitem/workbook/worksheets/Sheet1/tables/Table1/rows/add'

# Endpoint để overwrite file
update_url = f"https://graph.microsoft.com/v1.0/sites/fbdd4069-e12d-4a30-b316-926cebd4972e/drives/b!aUDd-y3hMEqzFpJs69SXLqsFboc6d3VHuXjQmhhH2yyWDIb9eEGSTJmyXz-tN3EO/root:/Dim Driver Event Daily.xlsx:/content"

# Access token
token_url = f'https://login.microsoftonline.com/a3f88450-77ef-4df3-89ea-c69cbc9bc410/oauth2/v2.0/token'
token_data = {
    'grant_type': 'client_credentials',
    'client_id': 'ad6b066a-d749-4f0b-bfbb-bad8de0af5d1',
    'client_secret': 'YwZ8Q~N6dAwc~sTcMAQsDQXwCKDfPBk81miLVbL4',
    'scope': 'https://graph.microsoft.com/.default'
}

token_r = requests.post(token_url, data=token_data)
access_token = token_r.json()['access_token']

# Headers
headers = {
    'Authorization': 'Bearer ' + access_token,
    'Content-Type': 'application/x-www-form-urlencoded',
}

# Đọc dữ liệu file và gửi lên SharePoint
# Thay đổi, cung  cấp dường dẫn của file đã lưu để mở file
with open('Dim Driver Event Daily.xlsx', 'rb') as file:
    file_content = file.read()
    response = requests.put(update_url, headers=headers, data=file_content)

if response.status_code == 200:
    print("ghi đè thông tin Dim Driver Event Daily thành công!")
else:
    print("Có lỗi xảy ra khi ghi đè thông tin.")
    
# ============================================================================================================================================================================

# ============================================================================================================================================================================

# ============================================================================================================================================================================
#Translate có detect những comments tiếng anh 
import requests
import pandas as pd
import time
from datetime import datetime
import html
from langdetect import detect

result_df = df
# Định nghĩa hàm sử dụng API của Google Translate
def make_api_request(url, params=None, headers=None):
    try:
        if headers is None:
            headers = {'Content-Type': 'application/json; charset=utf-8'}

        response = requests.get(url, params=params, headers=headers)

        if response.status_code == 200:
            translated_text = response.json()['data']['translations'][0]['translatedText']
            
            # Chuyển đổi HTML entities về dạng ký tự thường
            translated_text = html.unescape(translated_text)

            return translated_text
        else:
            return f"Error: {response.status_code} - {response.text}"

    except Exception as e:
        return f"An error occurred: {e}"

# Các bước trước khi dịch
current_date = datetime.today().strftime('%Y-%m-%d')
reviews_list = []

# Remove strip and blank rows
result_df['comments'] = result_df['comments'].apply(lambda x: '\n'.join(sentence.strip() for sentence in x.split('\n') if sentence.strip()))
# result_df['Review to English'] = result_df['Review to English'].apply(lambda x: '\n'.join(sentence.strip() for sentence in x.split('\n') if sentence.strip()))

# Lấy danh sách các comments theo rideScore cần dịch từ DataFrame
comment_list = result_df[result_df["rideScore"] != 'NA']["comments"].tolist()
translated_reviews = []  # List để lưu các bình luận đã dịch

percent = 0
for index, comment in enumerate(comment_list):
    
    try:
        # Phát hiện ngôn ngữ của bình luận
        frac_cmt_temp = comment.split('.')
        frac_cmt = '.'.join(reversed(frac_cmt_temp))
        language = detect(frac_cmt)

        
        # Nếu ngôn ngữ là tiếng Anh, giữ nguyên bình luận
        if language == 'en':
            translated_reviews.append(comment)
        else:
            api_url = "https://translation.googleapis.com/language/translate/v2"
            api_params = {
                'key': 'AIzaSyAd5eMHDpUjohF3uHBICaUKhUfiXzQkxNY',
                'target': 'en',
                'q': [comment]
            }
            translated_comment = make_api_request(api_url, params=api_params)
            
            # Lưu bình luận đã dịch vào danh sách
            translated_reviews.append(translated_comment)

        # Thêm delay sau mỗi 100 requests để tránh vượt quá giới hạn
        if index % 100 == 0:
            time.sleep(1)
        
        # Hiển thị tiến trình
        current_percent = round(index / len(comment_list) * 100)
        if current_percent != percent:
            print(f"{current_percent}%")
            percent = current_percent

    except Exception as e:
        print(f"Error at index {index}: {e}")
        translated_reviews.append(comment)

# Tạo DataFrame từ danh sách các bình luận đã dịch
df_translated = pd.DataFrame(translated_reviews, columns=['Review to English'])

# Lọc và chọn các cột cần thiết từ DataFrame gốc
df_filtered = result_df[result_df['rideScore'] != 'NA'][['Booking ID', 'comments']]

# Nối các DataFrame theo cột (axis=1)
resultReview = pd.concat([df_filtered.reset_index(drop=True), df_translated], axis=1)

# Gán giá trị ngày hiện tại cho cột 'timeStamp'
resultReview['timeStamp'] = current_date

# Thay đổi giá trị cho các comments "NA"
resultReview['Review to English'] = resultReview['Review to English'].replace('THAT', 'NA')


# Lưu DataFrame vào file Excel để kiểm tra
resultReview.to_excel('Review to English.xlsx', index=False)

# ============================================================================================================================================================================
# Append dữ liệu comments đã dịch cho cột Review to English bảng Dim_reviews sharepoint
import requests
import pandas as pd
append_url = f'https://graph.microsoft.com/v1.0/sites/fbdd4069-e12d-4a30-b316-926cebd4972e/lists/fd860c96-4178-4c92-99b2-5f3fad37710e/items/15/driveitem/workbook/worksheets/Sheet1/tables/Table1/rows/add'
# Access token
token_url = f'https://login.microsoftonline.com/a3f88450-77ef-4df3-89ea-c69cbc9bc410/oauth2/v2.0/token'
token_data = {
    'grant_type': 'client_credentials',
    'client_id': 'ad6b066a-d749-4f0b-bfbb-bad8de0af5d1',
    'client_secret': 'YwZ8Q~N6dAwc~sTcMAQsDQXwCKDfPBk81miLVbL4',
    'scope': 'https://graph.microsoft.com/.default'
}
token_r = requests.post(token_url, data=token_data)
access_token = token_r.json()['access_token']

# Headers
headers = {
    'Authorization': 'Bearer ' + access_token,
    'Content-Type': 'application/json',
}
data = resultReview
data = data.applymap(lambda x: str(x) if isinstance(x, float) else x)   

#Thay đổi giá trị 'nan'->'NA'
data['comments'] = data['comments'].replace('nan', 'NA')

data['Review to English'] = data['Review to English'].replace('nan', 'NA')

converted_data = data.values.tolist()


value = {"values": converted_data}

response = requests.post(append_url, headers=headers, json=value)
if response.status_code == 201:
    print("Apppend thông tin thành công!")       
else:    
    print("Có lỗi xảy ra khi Append thông tin.",{response.content})
    
# ============================================================================================================================================================================

# ============================================================================================================================================================================

# ============================================================================================================================================================================
    
df_preferred = pd.json_normalize(preferred_status['content'])

def map_status(status):
    if status:
        return 'On track'
    else:
        return 'At risk'
# Apply the mapping function to the 'preferred' column
df_preferred['preferred'] = df_preferred['preferred'].map(map_status)

df_preferred = df_preferred[['id', 'name', 'countryCode', 'preferred', 'declineRate.value', 'incidentRate.value', 'driverEventRate.value', 'averageSurveyScore.value']]
# Reorder cột theo thứ tự mới
new_order = ['countryCode', 'name', 'preferred', 'incidentRate.value', 'driverEventRate.value', 'declineRate.value', 'averageSurveyScore.value']
df_preferred = df_preferred[new_order]

# Đổi tên các cột
new_column_names = {
    'countryCode': 'Country',
    'name': 'Location name',
    'preferred': 'Preferred status',
    'incidentRate.value': 'Incident rate',
    'driverEventRate.value': 'Driver events',
    'declineRate.value': 'Decline rate',
    'averageSurveyScore.value': 'Avg. score'
}

# Sử dụng rename để đổi tên các cột
df_preferred = df_preferred.rename(columns=new_column_names)
df_preferred.to_excel("preferred.xlsx", index = False)

import requests

# Necessary information
tenant_id = 'a3f88450-77ef-4df3-89ea-c69cbc9bc410'
client_id = 'ad6b066a-d749-4f0b-bfbb-bad8de0af5d1'
client_secret = 'YwZ8Q~N6dAwc~sTcMAQsDQXwCKDfPBk81miLVbL4'  # Replace with the new client secret value
site_id = 'fbdd4069-e12d-4a30-b316-926cebd4972e'
drive_id = 'b!aUDd-y3hMEqzFpJs69SXLqsFboc6d3VHuXjQmhhH2yyWDIb9eEGSTJmyXz-tN3EO'

# Endpoint to overwrite the file
update_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/Prefer Partner by Country.xlsx:/content"

# Get access token
token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
token_data = {
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': client_secret,
    'scope': 'https://graph.microsoft.com/.default'
}

try:
    token_r = requests.post(token_url, data=token_data)
    token_r.raise_for_status()  # Ensure the request was successful
    access_token = token_r.json().get('access_token')
    
    if not access_token:
        print("Failed to obtain access token.")
        exit(1)
    else:
        print("Access token obtained successfully.")

    # Headers
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/octet-stream',
    }

    # Read file data and upload it to SharePoint
    file_path = 'preferred.xlsx'  # Ensure you provide the correct file path

    try:
        with open(file_path, 'rb') as file:
            file_content = file.read()
            response = requests.put(update_url, headers=headers, data=file_content)

        if response.status_code == 200:
            print("File successfully overwritten Preferred!")
        else:
            print(f"An error occurred: {response.status_code} - {response.text}")
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

except requests.exceptions.HTTPError as err:
    print(f"HTTP error occurred: {err}")
    print("Response content:", token_r.content)
except Exception as e:
    print(f"An error occurred: {e}")
