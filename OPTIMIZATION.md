# xlsx-importer v2-opt — Streaming Pipeline Optimization

## Подход: двухпроходной streaming с Consumer callback

Это **принципиально другой** подход по сравнению с v3:

| Версия | Парсер | Все строки в памяти? | Память для 100 МБ |
|--------|--------|---------------------|-------------------|
| v2 оригинал | XSSFWorkbook (DOM) | ДА | ~800 МБ |
| v3 | SAX streaming | ДА (в List) | ~200 МБ |
| **v2-opt** | **SAX + Consumer** | **НЕТ** | **~10–15 МБ** |

## Как работает

```
Проход 1: readMeta()
  xlsx → SAX → читает только строки 0-1 (имена и типы)
  → SheetMeta (в памяти только заголовок)
  → validateMeta() → CREATE TABLE на обоих нодах

Проход 2: streamRows()
  xlsx → SAX → строка готова → Consumer.accept(row)
                                  ↓
                            batch.add(row)
                            если batch.size() == 1000:
                              flushBatch() → ClickHouse
                              batch.clear()  ← тот же список, GC не нагружается
```

## Компромисс

Файл читается **дважды** — это ~10-20% потери по времени на SSD.
Зато heap можно установить `-Xmx256m` и обрабатывать файлы любого размера.

## Запуск с минимальным heap

```bash
java -Xmx256m -jar xlsx-importer.jar
```

Или в Dockerfile:
```dockerfile
ENV JAVA_OPTS="-Xms64m -Xmx256m -XX:+UseG1GC"
```
