import numpy as np
from numba import njit, types
#from numba.experimental import jitclass

FASTMATH = True

# Placeholder for CPF and CPF3 functions (to be implemented)
@njit(fastmath=FASTMATH)
def cpf_stub(x, y):
    # Example implementation using a simple approximation (replace with actual logic)
    # This is a dummy and may not be accurate
    z = np.complex128(x + 1j * y)
    w = np.exp(-z**2) * (1.0 - 1.0 / (1.0 + 2.0j * z))  # Placeholder for Faddeeva function
    return w.real, w.imag

@njit(fastmath=FASTMATH)
def cpf3_stub(x, y):
    # Similar to CPF but for different cases (replace with actual logic)
    return cpf(x, y)  # Placeholder

# Predefined constants using np.array for Numba compatibility
T = np.array([0.314240376, 0.947788391, 1.59768264, 2.27950708, 
              3.02063703, 3.8897249], dtype=np.float64)
U = np.array([1.01172805, -0.75197147, 0.012557727, 0.0100220082, 
              -0.000242068135, 0.000000500848061], dtype=np.float64)
S = np.array([1.393237, 0.231152406, -0.155351466, 0.00621836624, 
              0.0000919082986, -0.000000627525958], dtype=np.float64)
TT = np.array([0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 
               10.5, 11.5, 12.5, 13.5, 14.5], dtype=np.float64)
pipwoeronehalf = 0.564189583547756  # 1/√π constant

@njit(fastmath=FASTMATH)
def cpf(x, y):
    # Region 3 calculation for large magnitudes
    if np.sqrt(x**2 + y**2) > 8.0:
        zm1 = (1.0 + 0.0j) / complex(x, y)
        zm2 = zm1 ** 2
        zsum = 1.0 + 0.0j
        zterm = 1.0 + 0.0j
        for i in range(15):
            zterm *= zm2 * TT[i]
            zsum += zterm
        zsum *= 1j * zm1 * pipwoeronehalf
        return zsum.real, zsum.imag

    wr = 0.0
    wi = 0.0
    y1 = y + 1.5
    y2 = y1 ** 2

    # Region selection based on input conditions
    if (y > 0.85) or (abs(x) < (18.1 * y + 1.65)):
        # Region 1 calculations
        for i in range(6):
            t_i = T[i]
            u_i = U[i]
            s_i = S[i]
            
            # Negative T[i] terms
            r_neg = x - t_i
            d_neg = 1.0 / (r_neg**2 + y2)
            d1_neg = y1 * d_neg
            d2_neg = r_neg * d_neg
            
            # Positive T[i] terms
            r_pos = x + t_i
            d_pos = 1.0 / (r_pos**2 + y2)
            d1_pos = y1 * d_pos
            d2_pos = r_pos * d_pos
            
            # Accumulate results
            wr += u_i * (d1_neg + d1_pos) - s_i * (d2_neg - d2_pos)
            wi += u_i * (d2_neg + d2_pos) + s_i * (d1_neg - d1_pos)
    else:
        # Region 2 calculations
        if abs(x) < 12.0:
            wr = np.exp(-x**2)
        y3 = y + 3.0
        
        for i in range(6):
            t_i = T[i]
            u_i = U[i]
            s_i = S[i]
            
            # Negative T[i] terms
            r_neg = x - t_i
            r2_neg = r_neg**2
            d_neg = 1.0 / (r2_neg + y2)
            d1_neg = y1 * d_neg
            d2_neg = r_neg * d_neg
            term_wr_neg = y * (u_i * (r_neg * d2_neg - 1.5 * d1_neg) + 
                               s_i * y3 * d2_neg) / (r2_neg + 2.25)
            
            # Positive T[i] terms
            r_pos = x + t_i
            r2_pos = r_pos**2
            d_pos = 1.0 / (r2_pos + y2)
            d1_pos = y1 * d_pos
            d2_pos = r_pos * d_pos
            term_wr_pos = y * (u_i * (r_pos * d2_pos - 1.5 * d1_pos) - 
                               s_i * y3 * d2_pos) / (r2_pos + 2.25)
            
            # Accumulate results
            wr += term_wr_neg + term_wr_pos
            wi += u_i * (d2_neg + d2_pos) + s_i * (d1_neg - d1_pos)
    
    return wr, wi

@njit(fastmath=FASTMATH)
def cpf3(x, y):
    # Constants
    zone = np.complex128(1.0 + 0.0j)
    zi = np.complex128(0.0 + 1.0j)
    #tt = np.array([0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5,
    #               9.5, 10.5, 11.5, 12.5, 13.5, 14.5], dtype=np.float64)
    #pipwoeronehalf = np.float64(0.564189583547756)
    
    # Region 3 calculation
    zm1 = zone / np.complex128(x + 1j*y)
    zm2 = zm1 * zm1
    zsum = zone
    zterm = zone
    
    for i in range(15):
        zterm = zterm * zm2 * TT[i]
        zsum += zterm
    
    zsum *= zi * zm1 * pipwoeronehalf
    wr = np.real(zsum)
    wi = np.imag(zsum)
    
    return wr, wi

@njit(types.UniTuple(types.float64, 2)(
    types.float64, types.float64, types.float64, types.float64,
    types.float64, types.float64, types.float64, types.float64, types.float64
),fastmath=FASTMATH)
def pcqsdhc(sg0, GamD, Gam0, Gam2, Shift0, Shift2, anuVC, eta, sg):
    cte = np.sqrt(np.log(2.0)) / GamD
    pi = 4.0 * np.arctan(1.0)
    rpi = np.sqrt(pi)
    iz = np.complex128(0.0 + 1.0j)

    c0 = np.complex128(complex(Gam0, Shift0))
    c2 = np.complex128(complex(Gam2, Shift2))
    c0t = (1.0 - eta) * (c0 - 1.5 * c2) + anuVC
    c2t = (1.0 - eta) * c2

    if np.abs(c2t) == 0.0:
        Z1 = (iz * (sg0 - sg) + c0t) * cte
        xz1 = -np.imag(Z1)
        yz1 = np.real(Z1)
        wr1, wi1 = cpf(xz1, yz1)
        Aterm = rpi * cte * np.complex128(complex(wr1, wi1))
        if np.abs(Z1) <= 4e3:
            Bterm = rpi * cte * ((1.0 - Z1**2) * np.complex128(complex(wr1, wi1)) + Z1 / rpi)
        else:
            Bterm = cte * (rpi * np.complex128(complex(wr1, wi1)) + 0.5 / Z1 - 0.75 / (Z1**3))
    else:
        X = (iz * (sg0 - sg) + c0t) / c2t
        Y = 1.0 / (2.0 * cte * c2t)**2
        csqrtY = (Gam2 - iz * Shift2) / (2.0 * cte * (1.0 - eta) * (Gam2**2 + Shift2**2))
        if np.abs(X) <= 3e-8 * np.abs(Y):
            Z1 = (iz * (sg0 - sg) + c0t) * cte
            Z2 = np.sqrt(X + Y) + csqrtY
            xz1 = -np.imag(Z1)
            yz1 = np.real(Z1)
            xz2 = -np.imag(Z2)
            yz2 = np.real(Z2)
            wr1, wi1 = cpf(xz1, yz1)
            wr2, wi2 = cpf(xz2, yz2)
            Aterm = rpi * cte * (np.complex128(complex(wr1, wi1)) - np.complex128(complex(wr2, wi2)))
            Bterm = (-1.0 + rpi/(2.0*csqrtY)*(1.0-Z1**2)*np.complex128(complex(wr1, wi1)) -
                     rpi/(2.0*csqrtY)*(1.0-Z2**2)*np.complex128(complex(wr2, wi2))) / c2t
        elif np.abs(Y) <= 1e-15 * np.abs(X):
            Z = np.sqrt(X + Y)
            xz1 = -np.imag(Z)
            yz1 = np.real(Z)
            wr1, wi1 = cpf(xz1, yz1)
            if np.abs(np.sqrt(X)) <= 4e3:
                Zb = np.sqrt(X)
                xb = -np.imag(Zb)
                yb = np.real(Zb)
                wrb, wib = cpf(xb, yb)
                Aterm = (2.0 * rpi / c2t) * (1.0/rpi - np.sqrt(X) * np.complex128(complex(wrb, wib)))
                Bterm = (1.0 / c2t) * (-1.0 + 2.0*rpi*(1.0-X-2.0*Y)*(1.0/rpi - np.sqrt(X)*np.complex128(complex(wrb, wib))) +
                         2.0*rpi*np.sqrt(X+Y)*np.complex128(complex(wr1, wi1)))
            else:
                Aterm = (1.0 / c2t) * (1.0/X - 1.5/(X**2))
                Bterm = (1.0 / c2t) * (-1.0 + (1.0-X-2.0*Y)*(1.0/X - 1.5/(X**2)) +
                         2.0*rpi*np.sqrt(X+Y)*np.complex128(complex(wr1, wi1)))
        else:
            Z1 = np.sqrt(X + Y) - csqrtY
            Z2 = Z1 + 2.0 * csqrtY
            xz1 = -np.imag(Z1)
            yz1 = np.real(Z1)
            xz2 = -np.imag(Z2)
            yz2 = np.real(Z2)
            SZ1 = np.sqrt(xz1**2 + yz1**2)
            SZ2 = np.sqrt(xz2**2 + yz2**2)
            DSZ = np.abs(SZ1 - SZ2)
            SZmx = np.maximum(SZ1, SZ2)
            SZmn = np.minimum(SZ1, SZ2)
            if DSZ <= 1.0 and SZmx > 8.0 and SZmn <= 8.0:
                wr1, wi1 = cpf3(xz1, yz1)
                wr2, wi2 = cpf3(xz2, yz2)
            else:
                wr1, wi1 = cpf(xz1, yz1)
                wr2, wi2 = cpf(xz2, yz2)
            Aterm = rpi * cte * (np.complex128(complex(wr1, wi1)) - np.complex128(complex(wr2, wi2)))
            Bterm = (-1.0 + rpi/(2.0*csqrtY)*(1.0-Z1**2)*np.complex128(complex(wr1, wi1)) -
                     rpi/(2.0*csqrtY)*(1.0-Z2**2)*np.complex128(complex(wr2, wi2))) / c2t

    denominator = 1.0 - (anuVC - eta * (c0 - 1.5 * c2)) * Aterm + eta * c2 * Bterm
    LS_pCqSDHC = (1.0 / pi) * (Aterm / denominator)
    
    return LS_pCqSDHC.real, LS_pCqSDHC.imag

#@njit(fastmath=FASTMATH)
@njit(types.UniTuple(types.float64[:], 2)(
    types.float64, types.float64, types.float64, types.float64,
    types.float64, types.float64, types.float64, types.float64, types.float64[:]
),fastmath=FASTMATH)
def PROFILE_HT(sg0, GamD, Gam0, Gam2, Shift0, Shift2, anuVC, eta, sg):
    # Speed dependent Voigt profile based on HTP.
    # Input parameters:
    #      sg0     : Unperturbed line position in cm-1 (Input).
    #      GamD    : Doppler HWHM in cm-1 (Input)
    #      Gam0    : Speed-averaged line-width in cm-1 (Input).
    #      Gam2    : Speed dependence of the line-width in cm-1 (Input).
    #      Shift0  : Speed-averaged line-shift in cm-1 (Input).
    #      Shift2  : Speed dependence of the line-shift in cm-1 (Input)
    #      sg      : Current WaveNumber of the Computation in cm-1 (Input).
    n = len(sg)
    LS_pCqSDHC_R = np.zeros(n)
    LS_pCqSDHC_I = np.zeros(n)
    for i in range(n):
        LS_pCqSDHC_R[i],LS_pCqSDHC_I[i] = pcqsdhc(sg0,GamD,Gam0,Gam2,Shift0,Shift2,anuVC,eta,sg[i])
    return LS_pCqSDHC_R, LS_pCqSDHC_I

if __name__=="__main__":
    
    argline = '2000.1 0.01 0.5 0.1 1.0 0.1 0.2 0.01 2001.0'
    print('inputs:',argline)
    
    sg0,GamD,Gam0,Gam2,Shift0,Shift2,anuVC,eta,sg = \
        [float(arg) for arg in argline.split()]
    
    LS_pCqSDHC_R,LS_pCqSDHC_I = pcqsdhc(sg0,GamD,Gam0,Gam2,Shift0,Shift2,anuVC,eta,sg)
    
    print('outputs:',LS_pCqSDHC_R,LS_pCqSDHC_I)
