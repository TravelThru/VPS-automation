import requests
import pandas as pd
import time
import psutil
from seleniumwire import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

pd.set_option('display.max_columns', None)

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
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--allow-running-insecure-content")
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

        # Fill in the login details
        email_input.send_keys(email)
        password_input = driver.find_element(By.CSS_SELECTOR, "input[type='password'][name='password']")
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

###########################################################################################################################################################################
# API request to fetch ride data
url = "https://portal.taxi.booking.com/api/reports/rides"
base_url = "https://portal.taxi.booking.com/api/reports/rides"
params = {
    "pageSize": "500",
    "dateFrom": "2024-06-01T00:00:00",
    "dateTo": "2024-07-31T23:59:59",
    "supplierLocationIds": "",
    "hasRideRating": "false",
    "hasIncident": "false"
}
###########################################################################################################################################################################
# Headers
headers = {
    "Authorization": token
}
combined_data = []

num_pages = 500
for page in range(1, num_pages + 1):
    params["pageNumber"] = str(page)
    response = requests.get(base_url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        if 'results' in data and data['results']:
            combined_data.extend(data['results'])
        else:
            print(f"No data found on page {page}. Exiting loop.")
            break
    else:
        print(f"Failed to fetch data from page {page}. Status code:", response.status_code)
        break

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


# Trích xuất thông tin cần thiết từ dữ liệu JSON
bookings = combined_data
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
        "Supplier Location Name": supplierLocationName,
        "Ride Status": rideStatus,
        "Incident Status": incidentStatus,
        "Incident Type": incidentType,
        "Pickup Date Time UTC": pickupDateTimeUTC,
        "Pickup Date Time Local": pickupDateTimeLocal,
        "Driver's Name": driver_name,
        "Driver Events Status": fixed_status,
        "Price": fixed_price,
        "Currency": currency,
        "Driving Distance (km)": drivingDistanceInKm,
        "Pickup Location": pickupLocation,
        "Dropoff Location": dropoffLocation,
        "Timezone": timezone,
        "Rides Review": ridesReview,
        "Comments": comments,
        "Ride Score": rideScore,
        "Driver Departed to Pickup": departed_to_pickup,
        "Driver Arrived at Pickup": arrived_at_pickup,
        "Driver Departed to Dropoff": departed_to_dropoff,
        "Driver Arrived at Dropoff": arrived_at_dropoff,
        "Latitude Departed to Pickup": latitude_departed_to_pickup,
        "Longitude Departed to Pickup": longitude_departed_to_pickup,
        "Latitude Arrived at Pickup": latitude_arrived_at_pickup,
        "Longitude Arrived at Pickup": longitude_arrived_at_pickup,
        "Latitude Departed to Dropoff": latitude_departed_to_dropoff,
        "Longitude Departed to Dropoff": longitude_departed_to_dropoff,
        "Latitude Arrived at Dropoff": latitude_arrived_at_dropoff,
        "Longitude Arrived at Dropoff": longitude_arrived_at_dropoff,
        "Error Departed to Pickup": error_departed_to_pickup,
        "Error Arrived at Pickup": error_arrived_at_pickup,
        "Error Departed to Dropoff": error_departed_to_dropoff,
        "Error Arrived at Dropoff": error_arrived_at_dropoff
    }
    booking_info_list.append(booking_info)

# Tạo DataFrame từ danh sách thông tin booking
df = pd.DataFrame(booking_info_list)

# Tính toán cột Count dựa trên giá trị của các cột error
def calculate_error_percentage(row):
    error_departed_to_pickup = row['Error Departed to Pickup']
    error_arrived_at_pickup = row['Error Arrived at Pickup']
    error_departed_to_dropoff = row['Error Departed to Dropoff']
    error_arrived_at_dropoff = row['Error Arrived at Dropoff']

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

df.to_excel("Driver Performance VPS automation.xlsx")

#Overwrite lên file Driver Performance by ID New (SharePoint)
import requests

# Các thông tin cần thiết
tenant_id = 'a3f88450-77ef-4df3-89ea-c69cbc9bc410'
client_id = 'ad6b066a-d749-4f0b-bfbb-bad8de0af5d1'
client_secret = 'YwZ8Q~N6dAwc~sTcMAQsDQXwCKDfPBk81miLVbL4'
site_id = 'fbdd4069-e12d-4a30-b316-926cebd4972e'
drive_id = 'b!aUDd-y3hMEqzFpJs69SXLqsFboc6d3VHuXjQmhhH2yyWDIb9eEGSTJmyXz-tN3EO'

append_url = f'https://graph.microsoft.com/v1.0/sites/fbdd4069-e12d-4a30-b316-926cebd4972e/lists/fd860c96-4178-4c92-99b2-5f3fad37710e/items/15/driveitem/workbook/worksheets/Sheet1/tables/Table1/rows/add'

###########################################################################################################################################################################

# Endpoint để overwrite file
update_url = f"https://graph.microsoft.com/v1.0/sites/fbdd4069-e12d-4a30-b316-926cebd4972e/drives/b!aUDd-y3hMEqzFpJs69SXLqsFboc6d3VHuXjQmhhH2yyWDIb9eEGSTJmyXz-tN3EO/root:/Driver Performance VPS automation.xlsx:/content"

###########################################################################################################################################################################

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
###########################################################################################################################################################################
# Đọc dữ liệu file và gửi lên SharePoint
# Thay đổi, cung  cấp dường dẫn của file đã lưu để mở file
with open('Driver Performance VPS automation.xlsx', 'rb') as file:
    file_content = file.read()
    response = requests.put(update_url, headers=headers, data=file_content)

if response.status_code == 200:
    print("ghi đè thông tin thành công!")
else:
    print("Có lỗi xảy ra khi ghi đè thông tin.")
