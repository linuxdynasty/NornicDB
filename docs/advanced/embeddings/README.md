# Embeddings

**Generate and manage vector embeddings.**

## 📚 Documentation

- **[Local GGUF Models](local-gguf.md)** - Run models locally
- **[Ollama Integration](../../features/vector-embeddings.md#ollama-recommended)** - Use Ollama for embeddings
- **[OpenAI Integration](../../features/vector-embeddings.md#openai)** - Use OpenAI API

## 🎯 What are Embeddings?

Embeddings are vector representations of text that capture semantic meaning, enabling:
- Semantic search
- Similarity comparison
- K-Means clustering
- Classification

## 🚀 Quick Start

### With Ollama

```bash
# Start Ollama
ollama pull mxbai-embed-large

# Configure NornicDB
export NORNICDB_EMBEDDING_PROVIDER=ollama
export NORNICDB_EMBEDDING_MODEL=mxbai-embed-large
```

### With OpenAI

```bash
# Configure NornicDB
export NORNICDB_EMBEDDING_PROVIDER=openai
export NORNICDB_OPENAI_API_KEY=your-api-key
export NORNICDB_EMBEDDING_MODEL=text-embedding-3-small
```

## 📖 Learn More

- **[Local GGUF](local-gguf.md)** - Run models offline
- **[Ollama Setup](../../features/vector-embeddings.md#ollama-recommended)** - Easy local embeddings
- **[OpenAI API](../../features/vector-embeddings.md#openai)** - Cloud embeddings

---

**Get started** → **[Ollama Integration](../../features/vector-embeddings.md#ollama-recommended)**
