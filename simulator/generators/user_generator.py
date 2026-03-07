import uuid
import random
from datetime import datetime, timedelta, date
from faker import Faker

# =============================================================================
# User Generator Module — F1.1
# Purpose: Generate realistic user profiles matching defined demographics.
# =============================================================================


class UserGenerator:
    """
    Handles generation of realistic user profiles including demographic,
    geographic, technological, and behavioral data.
    """

    def __init__(self):
        """Initialize Faker and define weighted data mappings."""
        self.faker = Faker()

        # F1.1.1 - Identity Data Weights
        self.GENDER_CHOICES = ["male", "female", "other"]
        self.GENDER_WEIGHTS = [0.48, 0.48, 0.04]

        # F1.1.2 - Geographical Data Mappings
        self.COUNTRY_CHOICES = ["VN", "TH", "ID", "PH", "MY", "SG"]
        self.COUNTRY_WEIGHTS = [0.40, 0.20, 0.15, 0.10, 0.10, 0.05]

        self.GEO_CONFIG = {
            "VN": {
                "cities": [
                    "Ho Chi Minh City",
                    "Hanoi",
                    "Da Nang",
                    "Can Tho",
                    "Hai Phong",
                ],
                "tz": "Asia/Ho_Chi_Minh",
                "lang": "vi",
            },
            "TH": {
                "cities": ["Bangkok", "Chiang Mai", "Phuket", "Pattaya"],
                "tz": "Asia/Bangkok",
                "lang": "th",
            },
            "ID": {
                "cities": ["Jakarta", "Surabaya", "Bandung", "Medan"],
                "tz": "Asia/Jakarta",
                "lang": "id",
            },
            "PH": {
                "cities": ["Manila", "Cebu City", "Davao City"],
                "tz": "Asia/Manila",
                "lang": "en",
            },
            "MY": {
                "cities": ["Kuala Lumpur", "George Town", "Johor Bahru"],
                "tz": "Asia/Kuala_Lumpur",
                "lang": "en",
            },
            "SG": {"cities": ["Singapore"], "tz": "Asia/Singapore", "lang": "en"},
        }

        # F1.1.3 - Device & OS Mappings
        self.DEVICE_CHOICES = ["mobile", "desktop", "tablet"]
        self.DEVICE_WEIGHTS = [0.65, 0.25, 0.10]

        self.OS_MAPPING = {
            "mobile": ["Android", "iOS"],
            "desktop": ["Windows", "macOS", "Linux"],
            "tablet": ["iPadOS", "Android"],
        }

        # F1.1.4 - Behavior Mappings
        self.SEGMENT_CHOICES = ["new_user", "casual", "regular", "power_user", "vip"]
        self.FREQ_CHOICES = ["low", "medium", "high"]
        self.PRICE_SENS_CHOICES = ["budget", "mid_range", "premium"]

        # Used to generate bounded random dates
        self.now = datetime.utcnow()

    def _generate_identity(self) -> dict:
        """Generate base identity fields."""
        gender = random.choices(self.GENDER_CHOICES, weights=self.GENDER_WEIGHTS, k=1)[
            0
        ]

        # Map gender to faker name generation for realism
        if gender == "male":
            full_name = self.faker.name_male()
        elif gender == "female":
            full_name = self.faker.name_female()
        else:
            full_name = self.faker.name()

        username = self.faker.unique.user_name()

        # Age between 18 and 65
        start_date = date.today() - timedelta(days=65 * 365)
        end_date = date.today() - timedelta(days=18 * 365)
        dob = self.faker.date_between(start_date=start_date, end_date=end_date)

        return {
            "user_id": str(uuid.uuid4()),
            "username": username,
            "email": f"{username}@{self.faker.free_email_domain()}",
            "full_name": full_name,
            "gender": gender,
            "date_of_birth": dob.isoformat(),
            "avatar_url": f"https://ui-avatars.com/api/?name={username}&background=random",
        }

    def _generate_geography(self) -> dict:
        """Generate realistic geo fields maintaining country-city logic."""
        country = random.choices(
            self.COUNTRY_CHOICES, weights=self.COUNTRY_WEIGHTS, k=1
        )[0]
        geo_data = self.GEO_CONFIG[country]
        city = random.choice(geo_data["cities"])

        return {
            "country": country,
            "city": city,
            "timezone": geo_data["tz"],
            "preferred_language": geo_data["lang"],
        }

    def _generate_technology(self) -> dict:
        """Generate device details ensuring OS compatibility."""
        device = random.choices(self.DEVICE_CHOICES, weights=self.DEVICE_WEIGHTS, k=1)[
            0
        ]

        # Weighted OS choices for realism (e.g., more Windows than Mac on desktop)
        if device == "desktop":
            os_name = random.choices(
                self.OS_MAPPING["desktop"], weights=[0.75, 0.20, 0.05], k=1
            )[0]
        elif device == "mobile":
            os_name = random.choices(
                self.OS_MAPPING["mobile"], weights=[0.70, 0.30], k=1
            )[0]
        else:
            os_name = random.choices(
                self.OS_MAPPING["tablet"], weights=[0.80, 0.20], k=1
            )[0]

        return {"preferred_device": device, "os": os_name}

    def _generate_behavior(self) -> dict:
        """Generate user behavioral patterns and session metadata."""
        segment = random.choice(self.SEGMENT_CHOICES)

        # Base logic on segment to keep it realistic
        if segment in ["power_user", "vip"]:
            freq = "high"
            price_sens = random.choice(["mid_range", "premium"])
            avg_session = random.randint(30, 120)  # minutes
        elif segment == "new_user":
            freq = "low"
            price_sens = random.choice(self.PRICE_SENS_CHOICES)
            avg_session = random.randint(5, 20)
        else:
            freq = random.choice(["low", "medium"])
            price_sens = random.choice(self.PRICE_SENS_CHOICES)
            avg_session = random.randint(15, 60)

        # Registered any time in the last 3 years
        registered_at = self.now - timedelta(days=random.randint(1, 1095))

        # 85% of users are active, 15% churned/inactive
        is_active = random.random() < 0.85

        return {
            "user_segment": segment,
            "purchase_frequency": freq,
            "price_sensitivity": price_sens,
            "avg_session_duration_min": avg_session,
            "registered_at": registered_at.isoformat(),
            "is_active": is_active,
            "created_at": self.now.isoformat(),
        }

    def generate_user(self) -> dict:
        """
        Assemble and return a complete user profile dictionary.
        Returns:
            dict: Complete user payload matching the PostgreSQL schema.
        """
        user_profile = {}

        # Merge all partial dictionaries into one
        user_profile.update(self._generate_identity())
        user_profile.update(self._generate_geography())
        user_profile.update(self._generate_technology())
        user_profile.update(self._generate_behavior())

        return user_profile

    def generate_batch(self, count: int = 100) -> list:
        """
        Generate a list of user profiles efficiently.
        Args:
            count: Number of users to generate.
        Returns:
            list: List of user dictionaries.
        """
        return [self.generate_user() for _ in range(count)]


# For local testing module directly
if __name__ == "__main__":
    import json

    generator = UserGenerator()
    sample_user = generator.generate_user()
    print(json.dumps(sample_user, indent=2))
