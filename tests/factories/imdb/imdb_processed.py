import factory


class ImdbProcessedFactory(factory.Factory):
    class Meta:
        model = dict

    num_movies = 3  # Domyślna liczba filmów

    @factory.lazy_attribute
    def title(self):
        return {str(i): f"Movie Title {i}" for i in range(self.num_movies)}

    @factory.lazy_attribute
    def year(self):
        return {str(i): 2000.0 + i for i in range(self.num_movies)}

    @factory.lazy_attribute
    def vote(self):
        return {str(i): 1000.0 + i * 100.0 for i in range(self.num_movies)}

    @factory.lazy_attribute
    def runtime(self):
        return {str(i): 90.0 + i * 5.0 for i in range(self.num_movies)}

    @factory.lazy_attribute
    def rating(self):
        return {str(i): round(7.0 + i * 0.1, 1) for i in range(self.num_movies)}

    @factory.lazy_attribute
    def kind(self):
        kinds = ["movie", "video movie", "tv mini series", "tv series", "video game"]
        return {str(i): kinds[i % len(kinds)] for i in range(self.num_movies)}

    @factory.lazy_attribute
    def cast(self):
        return {
            str(i): [f"Actor {i}_1", f"Actor {i}_2", f"Actor {i}_3"]
            for i in range(self.num_movies)
        }

    @factory.lazy_attribute
    def director(self):
        return {str(i): [f"Director {i}"] for i in range(self.num_movies)}

    @factory.lazy_attribute
    def writer(self):
        return {
            str(i): [f"Writer {i}_1", f"Writer {i}_2"] for i in range(self.num_movies)
        }

    @factory.lazy_attribute
    def genre(self):
        return {
            str(i): [
                "Drama" if i % 3 == 0 else "Action",
                "Thriller" if i % 2 == 0 else "Comedy",
            ]
            for i in range(self.num_movies)
        }

    @factory.lazy_attribute
    def language(self):
        return {
            str(i): ["English", "Spanish"] if i % 2 == 0 else ["English"]
            for i in range(self.num_movies)
        }

    @factory.lazy_attribute
    def country(self):
        return {
            str(i): ["USA", "UK"] if i % 2 == 0 else ["USA"]
            for i in range(self.num_movies)
        }

    @factory.lazy_attribute
    def composer(self):
        return {str(i): [f"Composer {i}"] for i in range(self.num_movies)}

    @factory.post_generation
    def remove_num_movies(self, create, extracted, **kwargs):
        if "num_movies" in self:
            del self["num_movies"]
        return self
