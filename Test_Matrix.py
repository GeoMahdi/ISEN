import numpy as np
import scipy.linalg as slin
import sympy as sp
import networkx as nx
incidenceMatrix = np.array([[1,0,0,0,0,0],
                            [1,0,0,1,0,0],
                            [1,0,1,1,1,0],
                            [0,1,1,0,0,1],
                            [0,1,1,1,0,0]])
incidenceMatrixTranspose = np.transpose(incidenceMatrix)
ones = np.ones((5,5))
qNearMatrix = (np.matmul(incidenceMatrix,incidenceMatrixTranspose))-ones
print(qNearMatrix)
