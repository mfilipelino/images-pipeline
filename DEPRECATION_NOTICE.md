# Deprecation Notice

## Native K-means Implementation

The `native_kmeans` transformation option and the `native_kmeans_quantize` function are now **DEPRECATED** and will be removed in a future version.

### Why?

- The native Python implementation was created for educational purposes only
- It offers no performance benefits over the scikit-learn implementation
- The scikit-learn version is more robust and better maintained
- Maintaining two implementations increases code complexity without adding value

### Migration Guide

#### If you're using the CLI:

Replace:
```bash
--transformation native_kmeans
```

With:
```bash
--transformation kmeans
```

#### If you're using the functions directly:

Replace:
```python
from images_pipeline.core.image_utils import native_kmeans_quantize
result = native_kmeans_quantize(image, k=8)
```

With:
```python
from images_pipeline.core.image_utils import sklearn_kmeans_quantize
result = sklearn_kmeans_quantize(image, k=8)
```

Or preferably:
```python
from images_pipeline.core.image_utils import apply_transformation
result = apply_transformation(image, "kmeans")
```

### Timeline

- **Current**: Deprecation warnings are shown when using native_kmeans
- **Next major version**: The native_kmeans option will be removed entirely

### Performance Comparison

Based on our benchmarks:
- scikit-learn K-means: ~3-5x faster on average
- Better memory efficiency
- More accurate clustering results
- Handles edge cases better (empty clusters, convergence)