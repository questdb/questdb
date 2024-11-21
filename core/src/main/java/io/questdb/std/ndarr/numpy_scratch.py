#!/usr/bin/env -S uv run --no-project

# /// script
# dependencies = ["numpy", "scipy"]
# ///

from scipy.sparse import csr_matrix, csc_matrix

# Define the CSR components
row_pointers = [0, 1, 3, 4, 5]
column_indices = [2, 3, 0, 4, 1, 4]
values = [3, 4, 5, 7, 8, 9]

csr = csr_matrix((values, column_indices, row_pointers))
dense_array = csr.toarray()
print(dense_array)


def fmt_sparse(arr):
    f = lambda c: str(c.tolist()).replace("[", "{").replace("]", "}").replace(" ", "")
    return f(arr.indptr) + f(arr.indices) + f(arr.data)


# Create a CSR matrix from the dense array
csr = csr_matrix(dense_array)
print("CSR format:")
print("    Row pointers:   ", csr.indptr.tolist())
print("    Column indices: ", csr.indices.tolist())
print("    Values:         ", csr.data.tolist())
print("-> {{R{}}}".format(fmt_sparse(csr)))
print()

csc = csc_matrix(dense_array)
print("CSC format")
print("    Row pointers:   ", csc.indptr.tolist())
print("    Column indices: ", csc.indices.tolist())
print("    Values:         ", csc.data.tolist())
print("-> {{C{}}}".format(fmt_sparse(csc)))
print()

