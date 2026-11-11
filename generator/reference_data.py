"""Static reference data for the Zomato marketplace: cities, cuisines, menus, names."""

CITIES = {
    "Mumbai":    {"lat": 19.0760, "lon": 72.8777, "weight": 18,
                  "localities": ["Andheri West", "Bandra", "Powai", "Lower Parel", "Colaba", "Malad", "Thane West", "Juhu"]},
    "Delhi NCR": {"lat": 28.6139, "lon": 77.2090, "weight": 20,
                  "localities": ["Connaught Place", "Hauz Khas", "Saket", "Gurugram Sector 29", "Noida Sector 18",
                                 "Rajouri Garden", "Dwarka", "Lajpat Nagar"]},
    "Bengaluru": {"lat": 12.9716, "lon": 77.5946, "weight": 17,
                  "localities": ["Koramangala", "Indiranagar", "HSR Layout", "Whitefield", "Jayanagar", "Marathahalli",
                                 "BTM Layout", "MG Road"]},
    "Hyderabad": {"lat": 17.3850, "lon": 78.4867, "weight": 12,
                  "localities": ["Gachibowli", "Banjara Hills", "Jubilee Hills", "Madhapur", "Kondapur", "Ameerpet", "Kukatpally"]},
    "Chennai":   {"lat": 13.0827, "lon": 80.2707, "weight": 8,
                  "localities": ["T Nagar", "Anna Nagar", "Velachery", "Adyar", "OMR", "Nungambakkam"]},
    "Kolkata":   {"lat": 22.5726, "lon": 88.3639, "weight": 7,
                  "localities": ["Park Street", "Salt Lake", "New Town", "Ballygunge", "Gariahat"]},
    "Pune":      {"lat": 18.5204, "lon": 73.8567, "weight": 8,
                  "localities": ["Koregaon Park", "Viman Nagar", "Hinjewadi", "Baner", "Kothrud", "Wakad"]},
    "Ahmedabad": {"lat": 23.0225, "lon": 72.5714, "weight": 4,
                  "localities": ["Navrangpura", "Satellite", "Bodakdev", "Prahlad Nagar"]},
    "Jaipur":    {"lat": 26.9124, "lon": 75.7873, "weight": 3,
                  "localities": ["C Scheme", "Malviya Nagar", "Vaishali Nagar", "Raja Park"]},
    "Lucknow":   {"lat": 26.8467, "lon": 80.9462, "weight": 3,
                  "localities": ["Hazratganj", "Gomti Nagar", "Aliganj", "Indira Nagar"]},
}

# cuisine -> (popularity weight, [(menu item, base price INR)])
CUISINES = {
    "North Indian":  (16, [("Butter Chicken", 320), ("Dal Makhani", 240), ("Paneer Butter Masala", 260),
                           ("Butter Naan", 50), ("Jeera Rice", 150), ("Chole Bhature", 180)]),
    "Biryani":       (15, [("Chicken Dum Biryani", 299), ("Mutton Biryani", 399), ("Veg Biryani", 219),
                           ("Raita", 49), ("Chicken 65", 249)]),
    "South Indian":  (9,  [("Masala Dosa", 120), ("Idli Sambar", 80), ("Medu Vada", 70), ("Uttapam", 130),
                           ("Filter Coffee", 50)]),
    "Chinese":       (10, [("Hakka Noodles", 180), ("Chilli Chicken", 240), ("Veg Manchurian", 190),
                           ("Fried Rice", 170), ("Spring Rolls", 150)]),
    "Pizza":         (10, [("Margherita Pizza", 249), ("Farmhouse Pizza", 399), ("Pepperoni Pizza", 449),
                           ("Garlic Bread", 129), ("Choco Lava Cake", 109)]),
    "Burger":        (7,  [("Classic Veg Burger", 129), ("Chicken Burger", 179), ("Cheese Fries", 119),
                           ("Cold Coffee", 139)]),
    "Street Food":   (7,  [("Pav Bhaji", 140), ("Vada Pav", 40), ("Pani Puri", 60), ("Samosa Chaat", 80),
                           ("Dabeli", 50)]),
    "Desserts":      (6,  [("Gulab Jamun", 90), ("Rasmalai", 120), ("Brownie Sundae", 180), ("Kulfi", 80)]),
    "Healthy":       (4,  [("Quinoa Salad", 280), ("Grilled Chicken Bowl", 320), ("Smoothie Bowl", 240),
                           ("Protein Shake", 199)]),
    "Cafe":          (6,  [("Cappuccino", 160), ("Club Sandwich", 220), ("Pasta Alfredo", 290),
                           ("Blueberry Cheesecake", 240)]),
    "Mughlai":       (5,  [("Chicken Seekh Kebab", 280), ("Mutton Rogan Josh", 420), ("Galouti Kebab", 360),
                           ("Rumali Roti", 40)]),
    "Rolls & Wraps": (5,  [("Chicken Kathi Roll", 160), ("Paneer Tikka Roll", 140), ("Egg Roll", 90)]),
}

VEG_CUISINES = {"South Indian", "Street Food", "Desserts", "Cafe"}

NAME_PREFIXES = ["Spice", "Royal", "Urban", "Desi", "Tandoori", "Bombay", "Punjabi", "Madras", "Golden", "Green",
                 "The Hungry", "Saffron", "Masala", "Chai", "Mama's", "Shahi", "Nawab", "Bawarchi", "Swaad",
                 "Mirchi", "Zaika", "Rasoi", "Tadka", "Dilli", "Coastal", "Kolkata", "Lucky", "Paradise", "Moti"]
NAME_SUFFIXES = {
    "North Indian": ["Dhaba", "Rasoi", "Kitchen", "Da Dhaba", "Junction"],
    "Biryani": ["Biryani House", "Biryani Co.", "Darbar", "Handi", "Biryani Point"],
    "South Indian": ["Udupi", "Bhavan", "Tiffins", "Dosa Corner", "Idli House"],
    "Chinese": ["Wok", "Dragon", "Noodle Bar", "Chopsticks", "Chinese Corner"],
    "Pizza": ["Pizzeria", "Slice", "Oven", "Pizza Co.", "Crust"],
    "Burger": ["Burgers", "Grill", "Patty Shop", "Burger Barn"],
    "Street Food": ["Chaat Bhandar", "Pav Bhaji Centre", "Chowpatty", "Street Bites"],
    "Desserts": ["Sweets", "Mithai", "Creamery", "Dessert Bar"],
    "Healthy": ["Bowl Co.", "Greens", "Salad Bar", "Fit Kitchen"],
    "Cafe": ["Cafe", "Brew House", "Roastery", "Bistro"],
    "Mughlai": ["Kebab House", "Mughlai Darbar", "Tandoor", "Zaika-e-Awadh"],
    "Rolls & Wraps": ["Rolls", "Kathi Junction", "Wrap Station"],
}

FIRST_NAMES = ["Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh", "Krishna", "Ishaan", "Rohan",
               "Ananya", "Diya", "Saanvi", "Aadhya", "Pari", "Ira", "Myra", "Riya", "Kavya", "Sneha", "Rahul",
               "Priya", "Amit", "Neha", "Karan", "Pooja", "Siddharth", "Meera", "Farhan", "Zara", "Imran",
               "Ayesha", "Harpreet", "Gurpreet", "Lakshmi", "Venkat", "Suresh", "Divya", "Arnav", "Tanvi"]
LAST_NAMES = ["Sharma", "Verma", "Patel", "Iyer", "Reddy", "Nair", "Gupta", "Singh", "Khan", "Das", "Mukherjee",
              "Joshi", "Kulkarni", "Mehta", "Shah", "Rao", "Pillai", "Chopra", "Kapoor", "Bose", "Banerjee",
              "Menon", "Desai", "Naidu", "Qureshi", "Fernandes", "Ghosh", "Agarwal", "Saxena", "Yadav"]

PAYMENT_METHODS = [("UPI", 55), ("Card", 18), ("Cash on Delivery", 12), ("Wallet", 10), ("Net Banking", 5)]
PLATFORMS = [("android", 68), ("ios", 24), ("web", 8)]
COUPONS = [("WELCOME60", 0.60, 120), ("ZOMATO50", 0.50, 100), ("GOLD20", 0.20, 75), ("TRYNEW", 0.40, 80),
           ("SUPERSAVER", 0.30, 60)]
VEHICLES = [("Bike", 75), ("Scooter", 20), ("Bicycle", 3), ("EV Scooter", 2)]

RESTAURANT_CANCEL_REASONS = ["Restaurant closed", "Item out of stock", "Kitchen overloaded"]
CUSTOMER_CANCEL_REASONS = ["Changed mind", "Ordered by mistake", "Delivery taking too long", "Wrong address"]

REVIEW_TEXT = {
    5: ["Absolutely loved it!", "Hot, fresh and on time.", "Best biryani in town, hands down.", "Will order again."],
    4: ["Good food, slightly late.", "Tasty, decent portions.", "Nice packaging and taste."],
    3: ["Average experience.", "Food was okay, nothing special.", "Took longer than promised."],
    2: ["Food arrived cold.", "Portion was too small.", "Delivery was very late."],
    1: ["Terrible experience.", "Wrong order delivered.", "Never ordering from here again."],
}

# Relative demand by hour of day in IST (0..23): breakfast bump, lunch peak, evening snacks, dinner peak.
HOURLY_DEMAND_IST = [0.30, 0.18, 0.10, 0.06, 0.05, 0.08, 0.15, 0.30, 0.45, 0.50, 0.55, 0.80,
                     1.30, 1.50, 1.10, 0.70, 0.60, 0.75, 1.00, 1.40, 1.60, 1.50, 1.10, 0.55]


# ---------------------------------------------------------------------- city personalities
# Cuisine popularity multipliers per city (applied on top of CUISINES weights)
CITY_CUISINE_BIAS = {
    "Hyderabad": {"Biryani": 2.6, "Mughlai": 1.3},
    "Chennai":   {"South Indian": 3.2, "Biryani": 1.3},
    "Bengaluru": {"South Indian": 1.8, "Cafe": 1.8, "Healthy": 2.2, "Pizza": 1.2},
    "Kolkata":   {"Rolls & Wraps": 2.8, "Mughlai": 1.8, "Chinese": 1.6, "Desserts": 1.6},
    "Mumbai":    {"Street Food": 2.4, "Pizza": 1.2, "Cafe": 1.3},
    "Delhi NCR": {"North Indian": 1.6, "Mughlai": 1.6, "Street Food": 1.3},
    "Lucknow":   {"Mughlai": 3.0, "Biryani": 1.4},
    "Jaipur":    {"North Indian": 1.7, "Desserts": 1.4, "Street Food": 1.4},
    "Ahmedabad": {"Street Food": 2.0, "South Indian": 1.4, "Pizza": 1.3, "Mughlai": 0.3, "Biryani": 0.6},
    "Pune":      {"Cafe": 1.7, "Street Food": 1.5, "Burger": 1.3},
}
CITY_PRICE = {"Mumbai": 1.25, "Bengaluru": 1.15, "Delhi NCR": 1.15, "Hyderabad": 0.95, "Chennai": 0.95, "Pune": 1.05,
              "Kolkata": 0.85, "Ahmedabad": 0.9, "Jaipur": 0.85, "Lucknow": 0.8}
CITY_TRAFFIC_MIN = {"Bengaluru": 4, "Mumbai": 3, "Delhi NCR": 2, "Kolkata": 1, "Pune": 1, "Chennai": 0, "Hyderabad": 0,
                    "Ahmedabad": -1, "Lucknow": -1, "Jaipur": -2}
CITY_HOUR_SHIFT = {"Bengaluru": 1, "Mumbai": 1, "Hyderabad": 1, "Delhi NCR": 0, "Pune": 0, "Kolkata": 0,
                   "Chennai": -1, "Ahmedabad": -1, "Lucknow": -1, "Jaipur": 0}

# ---------------------------------------------------------------------- weather calendar
# Chance (per 2-minute check) that heavy rain hits a city in a given month. Follows the Indian
# seasons: southwest monsoon Jun-Sep (strongest on the west coast), withdrawal in October except
# Bengaluru/Hyderabad (October is Bengaluru's wettest month), and Chennai's northeast monsoon Oct-Dec.
RAIN_CHANCE = {
    "Mumbai":    {6: 0.55, 7: 0.75, 8: 0.65, 9: 0.45},              # monsoon withdraws ~Oct 10
    "Pune":      {6: 0.35, 7: 0.55, 8: 0.45, 9: 0.35, 10: 0.05},
    "Kolkata":   {5: 0.10, 6: 0.40, 7: 0.50, 8: 0.50, 9: 0.40, 10: 0.15},
    "Bengaluru": {5: 0.20, 6: 0.15, 7: 0.15, 8: 0.25, 9: 0.35, 10: 0.40, 11: 0.10},
    "Hyderabad": {6: 0.20, 7: 0.30, 8: 0.30, 9: 0.35, 10: 0.20},
    "Chennai":   {10: 0.40, 11: 0.55, 12: 0.35, 9: 0.10},
    "Delhi NCR": {7: 0.35, 8: 0.35, 9: 0.15},
    "Lucknow":   {7: 0.35, 8: 0.35, 9: 0.20},
    "Jaipur":    {7: 0.25, 8: 0.25, 9: 0.10},
    "Ahmedabad": {7: 0.35, 8: 0.30, 9: 0.15},
}
# Dry-day demand spikes: (label, demand multiplier, extra delivery minutes, extra cancellation)
NON_RAIN_EVENTS = [("Cricket match night", 1.5, 3, 0.0), ("Festival evening", 1.4, 5, 0.005), ("Long weekend", 1.25, 2, 0.0)]
RAIN_EVENT = ("Heavy rain", 1.6, 12, 0.02)
