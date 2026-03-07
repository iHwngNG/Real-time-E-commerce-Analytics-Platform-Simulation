import uuid
import random
from datetime import datetime, timedelta
from faker import Faker

# =============================================================================
# Product Generator Module — F1.2
# Purpose: Generate realistic product catalogs for seeding the PostgreSQL db.
# =============================================================================


class ProductGenerator:
    """
    Handles generation of a realistic e-commerce product catalog including:
    SKU generation, pricing logic, valid categories/subcategories, and metrics.
    """

    def __init__(self):
        """Initialize Faker and define category-specific product rules."""
        self.faker = Faker()
        self.now = datetime.utcnow()

        # F1.2.2 & F1.2.3 - Catalog Configuration with Realistic Pricing
        self.CATALOG_CONFIG = {
            "Electronics": {
                "abbr": "ELC",
                "subcats": ["Smartphones", "Laptops", "Tablets", "Audio", "Cameras"],
                "brands": ["Samsung", "Apple", "Sony", "Dell", "Xiaomi", "Asus"],
                "name_templates": [
                    "{brand} Pro {model}",
                    "{brand} Galaxy Ultra",
                    "{brand} ThinkPad XYZ",
                ],
                "price_range": (150.0, 3000.0),  # Premium pricing
            },
            "Fashion": {
                "abbr": "FAS",
                "subcats": [
                    "Men's Clothing",
                    "Women's Clothing",
                    "Shoes",
                    "Accessories",
                ],
                "brands": ["Nike", "Adidas", "Zara", "H&M", "Levi's", "Gucci"],
                "name_templates": [
                    "{brand} Classic T-Shirt",
                    "{brand} Running Shoes",
                    "{brand} Winter Jacket",
                ],
                "price_range": (15.0, 500.0),
            },
            "Beauty": {
                "abbr": "BTY",
                "subcats": ["Skincare", "Makeup", "Fragrance", "Haircare"],
                "brands": ["L'Oreal", "Estee Lauder", "Dior", "MAC", "The Ordinary"],
                "name_templates": [
                    "{brand} Anti-Aging Serum",
                    "{brand} Matte Lipstick",
                    "{brand} Eau de Parfum",
                ],
                "price_range": (10.0, 250.0),
            },
            "Home & Living": {
                "abbr": "HML",
                "subcats": ["Furniture", "Bedding", "Kitchenware", "Decor"],
                "brands": ["IKEA", "Philips", "Dyson", "Bosch", "Samsung"],
                "name_templates": [
                    "{brand} Smart Blender",
                    "{brand} Cozy Sofa",
                    "{brand} LED Desk Lamp",
                ],
                "price_range": (20.0, 1500.0),
            },
            "Sports": {
                "abbr": "SPT",
                "subcats": ["Fitness", "Outdoor", "Team Sports", "Cycling"],
                "brands": ["Under Armour", "Puma", "Decathlon", "Trek", "Wilson"],
                "name_templates": [
                    "{brand} Yoga Mat",
                    "{brand} Dumbbell Set",
                    "{brand} Mountain Bike",
                ],
                "price_range": (10.0, 1000.0),
            },
            "Food": {
                "abbr": "FOD",
                "subcats": ["Snacks", "Beverages", "Fresh Produce", "Pantry"],
                "brands": ["Nestle", "PepsiCo", "Coca-Cola", "Lay's", "Oreo"],
                "name_templates": [
                    "{brand} Organic Pack",
                    "{brand} Energy Drink",
                    "{brand} Premium Chocolate",
                ],
                "price_range": (1.0, 50.0),  # Cheaper items
            },
            "Books": {
                "abbr": "BKS",
                "subcats": ["Fiction", "Non-Fiction", "Educational", "Comics"],
                "brands": ["Penguin", "HarperCollins", "Oxford", "Marvel", "DC"],
                "name_templates": [
                    "{brand} Best Seller Novel",
                    "{brand} Illustrated Edition",
                    "{brand} Quick Guide",
                ],
                "price_range": (5.0, 100.0),
            },
            "Toys": {
                "abbr": "TOY",
                "subcats": ["Action Figures", "Board Games", "Educational", "Puzzles"],
                "brands": ["LEGO", "Hasbro", "Mattel", "Bandai", "Fisher-Price"],
                "name_templates": [
                    "{brand} Collector Set",
                    "{brand} Starter Kit",
                    "{brand} Ultimate Edition",
                ],
                "price_range": (10.0, 300.0),
            },
        }

        self.CURRENCY_CHOICES = ["VND"]
        self.TAG_POOL = [
            "bestseller",
            "new_arrival",
            "on_sale",
            "limited_edition",
            "eco_friendly",
            "premium",
        ]

    def _generate_price_and_stock(self, category_name: str) -> dict:
        """Calculate realistic original price, sale price, and discounts."""
        min_p, max_p = self.CATALOG_CONFIG[category_name]["price_range"]

        # Round original price to .99 or .00 for realism
        original_price = round(random.uniform(min_p, max_p)) - 0.01

        # Determine if item is on sale (30% chance)
        is_on_sale = random.random() < 0.30
        if is_on_sale:
            # Discount between 10% and 40% (PRD F1.2.3)
            discount_percent = round(random.uniform(10.0, 40.0), 2)
            sale_price = original_price * (1 - (discount_percent / 100))
            sale_price = round(sale_price, 2)
        else:
            discount_percent = 0.0
            sale_price = original_price

        return {
            "original_price": original_price,
            "sale_price": sale_price,
            "discount_percent": discount_percent,
            "stock_quantity": random.randint(0, 10000),
            "currency": random.choice(self.CURRENCY_CHOICES),
        }

    def _generate_metrics_and_metadata(self, stock_quantity: int) -> dict:
        """Generate popularity score and ratings."""
        # Weighted rating to skew towards positive reviews
        rating_avg = round(random.uniform(3.5, 5.0), 2)
        rating_count = random.randint(0, 5000)

        return {
            "rating_avg": rating_avg,
            "rating_count": rating_count,
            "is_available": stock_quantity > 0,
            "popularity_score": random.randint(
                0, 100
            ),  # Used for weight picking during streams
            "created_at": (
                self.now - timedelta(days=random.randint(1, 700))
            ).isoformat(),
            "updated_at": self.now.isoformat(),
        }

    def generate_product(self) -> dict:
        """
        Assemble and return a complete product dictionary.
        Returns:
            dict: Complete product payload matching PostgreSQL schema.
        """
        category_name = random.choice(list(self.CATALOG_CONFIG.keys()))
        config = self.CATALOG_CONFIG[category_name]

        # Identity
        brand = random.choice(config["brands"])
        template = random.choice(config["name_templates"])
        # Format name with brand and random fake word to simulate model name
        product_name = template.format(
            brand=brand, model=self.faker.word().capitalize()
        )

        # SKU Structure: ELC-849201
        sku = f"{config['abbr']}-{random.randint(100000, 999999)}"

        # Taxonomy
        subcategory = random.choice(config["subcats"])
        # Select 1 to 3 random tags
        tags = random.sample(self.TAG_POOL, k=random.randint(1, 3))

        price_stock = self._generate_price_and_stock(category_name)
        metrics = self._generate_metrics_and_metadata(price_stock["stock_quantity"])

        product_profile = {
            "product_id": str(uuid.uuid4()),
            "sku": sku,
            "product_name": product_name,
            "brand": brand,
            "description": self.faker.paragraph(nb_sentences=random.randint(1, 3)),
            "image_url": f"https://via.placeholder.com/400x400.png?text={config['abbr']}",
            "category_name": category_name,
            "subcategory": subcategory,
            "tags": tags,
        }

        product_profile.update(price_stock)
        product_profile.update(metrics)

        return product_profile

    def generate_batch(self, count: int = 100) -> list:
        """
        Generate a list of products efficiently.
        Args:
            count: Number of products to generate.
        Returns:
            list: List of product dictionaries.
        """
        return [self.generate_product() for _ in range(count)]


# For local testing module directly
if __name__ == "__main__":
    import json

    generator = ProductGenerator()
    sample_product = generator.generate_product()
    print(json.dumps(sample_product, indent=2))
