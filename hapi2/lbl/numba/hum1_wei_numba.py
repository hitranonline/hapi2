from numba import njit
import numpy as np
from numpy.fft import fft, fftshift

from .settings import FASTMATH

def compute_L_a(N=24):
    # Computes the function w(z) = exp(-zA2) erfc(-iz) using a rational
    # series with N terms. It is assumed that Im(z) > 0 or Im(z) = 0.
    M = 2*N; M2 = 2*M; k = np.arange(-M+1,M) #'; # M2 = no. of sampling points.
    L = np.sqrt(N/np.sqrt(2)); # Optimal choice of L.
    theta = k*np.pi/M; t = L*np.tan(theta/2); # Variables theta and t.
    #f = exp(-t.A2)*(LA2+t.A2); f = [0; f]; # Function to be transformed.
    f = np.zeros(len(t)+1); f[0] = 0
    f[1:] = np.exp(-t**2)*(L**2+t**2)
    #f = insert(exp(-t**2)*(L**2+t**2),0,0)
    a = np.real(fft(fftshift(f)))/M2; # Coefficients of transform.
    a = np.flipud(a[1:N+1]); # Reorder coefficients.
    return L,a
    
#L,a = compute_L_a(N=24)
#"""
L=4.119534287814235e+00
a=[ -1.513746165452782e-10,
     4.904820475696662e-09,
     1.331045371144590e-09,
    -3.008282354906122e-08,
    -1.912225894758664e-08,
     1.873834344680766e-07,
     2.568264133855852e-07,
    -1.085647579534184e-06,
    -3.038893184193909e-06,
     4.139461724262039e-06,
     3.047106608303641e-05,
     2.433141546226158e-05,
    -2.074843151142445e-04,
    -7.816642995623771e-04,
    -4.936426901285609e-04,
     6.215006362949158e-03,
     3.372336685531599e-02,
     1.083872348456672e-01,
     2.654963959880770e-01,
     5.361139535729114e-01,
     9.257087138588674e-01,
     1.394819673379119e+00,
     1.856286499205541e+00,
     2.197858936531541e+00,] 
#""";
a = np.flip(a,axis=0)     
    
@njit(fastmath=FASTMATH)   # this function should receive a scalar arguments
def cef(x,y,L,a):
    z = x + 1.0j*y
    Z = (L+1.0j*z)/(L-1.0j*z); #p = polyval(a,Z); # Polynomial evaluation.

    # embedding polyval implementation
    p = 0.0
    zp = 1.0
    #for i, v in enumerate(a):
    for v in a:
        #p += v*Z**i
        #p += v*Z**(N-1-i)
        p += v*zp
        zp *= Z
    #print(p)
    
    w = 2*p/(L-1.0j*z)**2+(1/np.sqrt(np.pi))/(L-1.0j*z); # Evaluate w(z).
    return w

recSqrtPi = 1/np.sqrt(np.pi)
    
#"""    
@njit(fastmath=FASTMATH)   # Converted from Fortran version of the paper
def hum1_wei(x,y):    
    cerf = 0+1.0j
    t = y-1.0j*x
    if y>15.0 or x>15.0-y or x<y-15.0:        
        cerf = recSqrtPi*t/(0.5+t**2)
    else:
        if np.abs(x)>15-y:
            cerf = 1/np.sqrt(np.pi)*t/(0.5+t**2)
        else:
            cerf = cef(x,y,L,a)
    return cerf.real,cerf.imag  
#"""

"""
@njit(fastmath=FASTMATH)   # this function should receive a scalar arguments
def hum1_wei0(x,y):
    L = 4.119534287814235e+00
    a = np.array([ 
         2.197858936531541e+00,
         1.856286499205541e+00,
         1.394819673379119e+00,
         9.257087138588674e-01,
         5.361139535729114e-01,
         2.654963959880770e-01,
         1.083872348456672e-01,
         3.372336685531599e-02,
         6.215006362949158e-03,
        -4.936426901285609e-04,
        -7.816642995623771e-04,
        -2.074843151142445e-04,
         2.433141546226158e-05,
         3.047106608303641e-05,
         4.139461724262039e-06,
        -3.038893184193909e-06,
        -1.085647579534184e-06,
         2.568264133855852e-07,
        -1.912225894758664e-08,
        -3.008282354906122e-08,
         1.331045371144590e-09,
         4.904820475696662e-09,
        -1.513746165452782e-10,
        ]) 
    if np.abs(x)+y<15.0:
        t = y-1.0j*x
        cerf = 1/np.sqrt(np.pi)*t/(0.5+t**2)
    else:
        cerf = cef(x,y,L,a)
    return cerf.real,cerf.imag  
"""