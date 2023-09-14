"""
Document template for the docloader
"""
import json
import random
import string


class Rating:
    """
    Stores rating for class Review
    """

    def __init__(self):
        self.value = None
        self.cleanliness = None
        self.overall = None


class Review:
    """
    Stores review for class Hotel
    params:
    -faker_instance
    """

    def __init__(self, faker_instance):
        self.date = faker_instance.date_time_between(start_date="-10y", end_date="now").isoformat()
        self.author = faker_instance.name()
        self.rating = Rating()


class Hotel:
    """
    Stores Hotel information to generate document for the doc_loader
    """

    def __init__(self, faker_instance):
        self.document_size = None
        self.country = faker_instance.country()
        self.address = faker_instance.address()
        self.free_parking = int(faker_instance.boolean())
        self.city = faker_instance.city()
        self.type = "Hotel"
        self.url = faker_instance.url()
        self.reviews = []
        self.phone = faker_instance.phone_number()
        self.price = faker_instance.random_element(
            elements=(1000.0, 2000.0, 3000.0, 4000.0, 5000.0, 6000.0,
                      7000.0, 8000.0, 9000.0, 10000.0))
        self.avg_ratings = faker_instance.pyfloat(left_digits=1, right_digits=1, positive=True)
        self.free_breakfast = int(faker_instance.boolean())
        self.name = faker_instance.name()
        self.public_likes = []
        self.email = faker_instance.email()
        self.mutated = 0.0
        self.padding = ""
        self.key = None

    def generate_review(self, faker_instance):
        """
        Generated random Review for a hotel
        :param faker_instance:
        :return: object of class Review
        """
        review = Review(faker_instance)
        review.rating.value = faker_instance.random_int(min=0, max=10)
        review.rating.cleanliness = faker_instance.random_int(min=0, max=10)
        review.rating.overall = faker_instance.random_int(min=1, max=10)
        return review

    def generate_public_likes(self, faker_instance):
        """
        Generated random value for variable self.PublicLikes
        :param faker_instance:
        """
        num_likes = faker_instance.random_int(min=0, max=10)
        self.public_likes = [faker_instance.name() for _ in range(num_likes)]

    def generate_document(self, faker_instance, document_size, key=None):
        """
        Generates document os a given size in bytes.
        :param key: in-case of custom key requirement
        :param faker_instance:
        :param document_size:
        """
        self.reviews = []
        self.key = key
        self.document_size = document_size
        self.generate_public_likes(faker_instance)
        while True:
            new_review = self.generate_review(faker_instance)
            document = json.dumps(self.__dict__, default=lambda x: x.__dict__,
                                  ensure_ascii=False)
            new_review_doc = json.dumps(new_review.__dict__, default=lambda x: x.__dict__,
                                        ensure_ascii=False)
            if len(document.encode("utf-8")) + len(new_review_doc.encode("utf-8")) <= document_size:
                self.reviews.append(new_review)
            else:
                required_length = document_size - len(document.encode("utf-8"))
                self.padding = ''.join(random.choices(string.ascii_letters, k=required_length))
                break
