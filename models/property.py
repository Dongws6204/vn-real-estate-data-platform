from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass

@dataclass
class Location:
    address: str
    district: str
    city: str
    coordinates: Optional[Dict[str, float]] = None

@dataclass
class Price:
    amount: float
    currency: str = "VND"
    unit: str = "total"

@dataclass
class Features:
    area: float
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    floors: Optional[int] = None

@dataclass
class ContactInfo:
    name: str
    phone: Optional[str] = None
    email: Optional[str] = None

@dataclass
class Media:
    images: List[str]
    videos: List[str]

@dataclass
class Property:
    source: str
    source_id: str
    url: str
    title: str
    price: Price
    location: Location
    property_type: str
    description: str
    features: Features
    contact_info: ContactInfo
    media: Media
    posted_date: datetime
    crawled_at: datetime
    updated_at: datetime
    status: str

    def to_dict(self) -> Dict:
        return {
            'source': self.source,
            'source_id': self.source_id,
            'url': self.url,
            'title': self.title,
            'price': {
                'amount': self.price.amount,
                'currency': self.price.currency,
                'unit': self.price.unit
            },
            'location': {
                'address': self.location.address,
                'district': self.location.district,
                'city': self.location.city,
                'coordinates': self.location.coordinates
            },
            'property_type': self.property_type,
            'description': self.description,
            'features': {
                'area': self.features.area,
                'bedrooms': self.features.bedrooms,
                'bathrooms': self.features.bathrooms,
                'floors': self.features.floors
            },
            'contact_info': {
                'name': self.contact_info.name,
                'phone': self.contact_info.phone,
                'email': self.contact_info.email
            },
            'media': {
                'images': self.media.images,
                'videos': self.media.videos
            },
            'posted_date': self.posted_date,
            'crawled_at': self.crawled_at,
            'updated_at': self.updated_at,
            'status': self.status
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'Property':
        return cls(
            source=data['source'],
            source_id=data['source_id'],
            url=data['url'],
            title=data['title'],
            price=Price(**data['price']),
            location=Location(**data['location']),
            property_type=data['property_type'],
            description=data['description'],
            features=Features(**data['features']),
            contact_info=ContactInfo(**data['contact_info']),
            media=Media(**data['media']),
            posted_date=data['posted_date'],
            crawled_at=data['crawled_at'],
            updated_at=data['updated_at'],
            status=data['status']
        )