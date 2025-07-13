import requests
import re
from collections import defaultdict
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor # Для багатопотоковості

# Приклад реалізації MapReduce (з адаптаціями)
class MapReduce:
    def __init__(self, num_mappers=4, num_reducers=2):
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers

    def map(self, text_chunk):
        words = re.findall(r'\b\w+\b', text_chunk.lower())
        return [(word, 1) for word in words]

    def shuffle_and_sort(self, mapped_values):
        shuffled = defaultdict(list)
        for key, value in mapped_values:
            shuffled[key].append(value)
        return shuffled

    def reduce(self, key, values):
        return (key, sum(values))

    def run(self, data):
        # Розділення даних на чанки для маперів
        chunk_size = (len(data) + self.num_mappers - 1) // self.num_mappers
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

        mapped_results = []
        with ThreadPoolExecutor(max_workers=self.num_mappers) as mapper_pool:
            # Використання багатопотоковості для фази Map
            futures = [mapper_pool.submit(self.map, chunk) for chunk in chunks]
            for future in futures:
                mapped_results.extend(future.result())

        shuffled_data = self.shuffle_and_sort(mapped_results)

        reduced_results = []
        # Розподіл ключів між редьюсерами
        keys_per_reducer = defaultdict(list)
        all_keys = list(shuffled_data.keys())
        for i, key in enumerate(all_keys):
            keys_per_reducer[i % self.num_reducers].append(key)

        with ThreadPoolExecutor(max_workers=self.num_reducers) as reducer_pool:
            # Використання багатопотоковості для фази Reduce
            futures = []
            for reducer_id in range(self.num_reducers):
                for key in keys_per_reducer[reducer_id]:
                    futures.append(reducer_pool.submit(self.reduce, key, shuffled_data[key]))
            for future in futures:
                reduced_results.append(future.result())

        return dict(reduced_results)

def get_text_from_url(url: str) -> str:
    """
    Завантажує текст із заданої URL-адреси.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Викликає HTTPError для поганих відповідей (4xx або 5xx)
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Помилка під час завантаження тексту з {url}: {e}")
        return ""

def visualize_top_words(word_frequencies: dict, top_n: int = 10):
    """
    Візуалізує топ-слова з найвищою частотою використання.
    """
    # Сортуємо слова за частотою у спадному порядку
    sorted_words = sorted(word_frequencies.items(), key=lambda item: item[1], reverse=True)
    top_words = sorted_words[:top_n]

    words = [word for word, freq in top_words]
    frequencies = [freq for word, freq in top_words]

    plt.figure(figsize=(10, 6))
    plt.barh(words[::-1], frequencies[::-1], color='skyblue') # Розвертаємо для кращої візуалізації
    plt.xlabel("Частота")
    plt.ylabel("Слово")
    plt.title(f"Топ {top_n} найбільш вживаних слів")
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    # Приклад URL для тестування
    # Можна замінити на будь-яку іншу URL, що містить текст
    url = "https://www.gutenberg.org/files/1342/1342-0.txt" # Приклад: Pride and Prejudice by Jane Austen

    print(f"Завантаження тексту з {url}...")
    text = get_text_from_url(url)

    if text:
        print("Текст успішно завантажено. Виконується MapReduce аналіз...")
        mr = MapReduce(num_mappers=4, num_reducers=2) # Використання 4 маперів та 2 редьюсерів
        word_counts = mr.run(text)

        print("Аналіз завершено. Візуалізація результатів...")
        visualize_top_words(word_counts, top_n=10)
    else:
        print("Не вдалося завантажити текст. Візуалізація неможлива.")
