import os
"""
os.environ[''] = 
os.environ[''] = 
os.environ[''] = 
os.environ[''] = 
os.environ[''] = 
os.environ[''] = 
"""

import numpy as np
from numba import njit
from numpy.fft import fft, fftshift

from .settings import FASTMATH
from .intrinsics import *
#from fadf import fadf # CPF by Abrarov et al (2018)
from .hum1_wei_numba import hum1_wei 

T = np.array([0.314240376E0,0.947788391E0,1.59768264E0,2.27950708E0,3.02063703E0,3.8897249E0])
U = np.array([1.01172805E0,-0.75197147E0,1.2557727E-2,1.00220082E-2,-2.42068135E-4,5.00848061E-7])
S = np.array([1.393237E0,0.231152406E0,-0.155351466E0,6.21836624E-3,9.19082986E-5,-6.27525958E-7])
zone = dcmplx(1.0E0,0.0E0)
zi = dcmplx(0.0E0,1.0E0)
tt = np.array([0.5E0,1.5E0,2.5E0,3.5E0,4.5E0,5.5E0,6.5E0,7.5E0,8.5E0,9.5E0,10.5E0,11.5E0,12.5E0,13.5E0,14.5E0])
pipwoeronehalf = 0.564189583547756E0

#@njit
#def polyval(p, x):
#    y = np.zeros(x.shape, dtype=float)
#    for i, v in enumerate(p):
#        y *= x
#        y += v
#    return y

# WEIDEMANN'S APPROXIMATION (USED IN SCHREIER'S CPF)
# http://appliedmaths.sun.ac.za/~weideman/research/cef.html
    
#def cefOLD(x,y,N):
#    # Computes the function w(z) = exp(-zA2) erfc(-iz) using a rational
#    # series with N terms. It is assumed that Im(z) > 0 or Im(z) = 0.
#    z = x + 1.0j*y
#    M = 2*N; M2 = 2*M; k = np.arange(-M+1,M) #'; # M2 = no. of sampling points.
#    L = np.sqrt(N/np.sqrt(2)); # Optimal choice of L.
#    theta = k*np.pi/M; t = L*np.tan(theta/2); # Variables theta and t.
#    #f = exp(-t.A2)*(LA2+t.A2); f = [0; f]; # Function to be transformed.
#    f = np.zeros(len(t)+1); f[0] = 0
#    f[1:] = np.exp(-t**2)*(L**2+t**2)
#    #f = insert(exp(-t**2)*(L**2+t**2),0,0)
#    tmp = fft(fftshift(f))
#    a = tmp.real/M2; # Coefficients of transform.
#    a = np.flipud(a[1:N+1]); # Reorder coefficients.
#    Z = (L+1.0j*z)/(L-1.0j*z); p = np.polyval(a,Z); # Polynomial evaluation.
#    w = 2*p/(L-1.0j*z)**2+(1/np.sqrt(np.pi))/(L-1.0j*z); # Evaluate w(z).
#    return w

#def compute_L_a(N=24):
#    # Computes the function w(z) = exp(-zA2) erfc(-iz) using a rational
#    # series with N terms. It is assumed that Im(z) > 0 or Im(z) = 0.
#    M = 2*N; M2 = 2*M; k = np.arange(-M+1,M) #'; # M2 = no. of sampling points.
#    L = np.sqrt(N/np.sqrt(2)); # Optimal choice of L.
#    theta = k*np.pi/M; t = L*np.tan(theta/2); # Variables theta and t.
#    #f = exp(-t.A2)*(LA2+t.A2); f = [0; f]; # Function to be transformed.
#    f = np.zeros(len(t)+1); f[0] = 0
#    f[1:] = np.exp(-t**2)*(L**2+t**2)
#    #f = insert(exp(-t**2)*(L**2+t**2),0,0)
#    a = np.real(fft(fftshift(f)))/M2; # Coefficients of transform.
#    a = np.flipud(a[1:N+1]); # Reorder coefficients.
#    return L,a
    
#L,a = compute_L_a(N=24)
    
#@njit
#def cef_vector(x,y,L,a):
#    z = x + 1.0j*y
#    Z = (L+1.0j*z)/(L-1.0j*z); #p = polyval(a,Z); # Polynomial evaluation.
#
#    # embedding polyval implementation
#    p = np.empty(len(Z),dtype=np.complex128)
#    for i, v in enumerate(a):
#        p += v*Z**i
#    
#    w = 2*p/(L-1.0j*z)**2+(1/np.sqrt(np.pi))/(L-1.0j*z); # Evaluate w(z).
#    return w

#@njit(fastmath=FASTMATH)   # this function should receive a scalar arguments
#def cef(x,y,L,a):
#    z = x + 1.0j*y
#    Z = (L+1.0j*z)/(L-1.0j*z); #p = polyval(a,Z); # Polynomial evaluation.
#
#    # embedding polyval implementation
#    p = 0
#    for i, v in enumerate(a):
#        p += v*Z**i
#    
#    w = 2*p/(L-1.0j*z)**2+(1/np.sqrt(np.pi))/(L-1.0j*z); # Evaluate w(z).
#    return w
    
#@njit
#def hum1_weiOLD(x,y):
#    t = y-1.0j*x
#    cerf=1/np.sqrt(np.pi)*t/(0.5+t**2)
#    """
#    z = x+1j*y
#    cerf = 1j*z/sqrt(pi)/(z**2-0.5)
#    """
#    mask = abs(x)+y<15.0
#    if any(mask):
#        w24 = cef(x[mask],y[mask],L,a)
#        np.place(cerf,mask,w24)
#    return cerf.real,cerf.imag

#@njit
#def hum1_wei_vectorized(x,y):
#    nx = len(x)
#    cerf = np.empty(nx,dtype=np.complex128)
#    if y>15 or np.min(x)>15-y or np.max(x)<y-15:
#        for i in range(nx):
#            t = y-1.0j*x[i]
#            cerf[i] = 1/np.sqrt(np.pi)*t/(0.5+t**2)
#    else:
#        for i in range(nx):
#            t = y-1.0j*x[i]
#            if np.abs(x[i])>15-y:
#                cerf[i] = 1/np.sqrt(np.pi)*t/(0.5+t**2)
#            else:
#                cerf[i] = cef(x[i],y,L,a)
#    return cerf    

#@njit(fastmath=FASTMATH)   # this function should receive a scalar arguments
#def hum1_wei(x,y):
#    t = y-1.0j*x
#    if y>15 or x>15-y or x<y-15:
#        cerf = 1/np.sqrt(np.pi)*t/(0.5+t**2)
#    else:
#        if np.abs(x)>15-y:
#            cerf = 1/np.sqrt(np.pi)*t/(0.5+t**2)
#        else:
#            cerf = cef(x,y,L,a)
#    return cerf.real,cerf.imag   
    
@njit(fastmath=FASTMATH)
def HTP_CPF(X,Y): # => WR,WI
#C-------------------------------------------------
#C "CPF": Complex Probability Function
#C .........................................................
#C         .       Subroutine to Compute the Complex       .
#C         .        Probability Function W(z=X+iY)         .
#C         .     W(z)=exp(-z**2)*Erfc(-i*z) with Y>=0      .
#C         .    Which Appears when Convoluting a Complex   .
#C         .     Lorentzian Profile by a Gaussian Shape    .
#C         .................................................
#C
#C             WR : Real Part of W(z)
#C             WI : Imaginary Part of W(z)
#C
#C This Routine was Taken from the Paper by J. Humlicek, which 
#C is Available in Page 309 of Volume 21 of the 1979 Issue of
#C the Journal of Quantitative Spectroscopy and Radiative Transfer
#C Please Refer to this Paper for More Information
#C
#C Accessed Files:  None
#C --------------
#C
#C Called Routines: None                               
#C ---------------                                 
#C
#C Called By: 'CompAbs' (COMPute ABSorpton)
#C ---------
#C
#C Double Precision Version
#C
#C-------------------------------------------------
#C      
#      Implicit None
#        Integer I
#	double complex zm1,zm2,zterm,zsum,zone,zi
#      Double Precision X,Y,WR,WI
#      Double Precision T,U,S,Y1,Y2,Y3,R,R2,D,D1,D2,D3,D4
#      Double Precision TT(15),pipwoeronehalf
#C      
#      Dimension T(6),U(6),S(6)
#      Data T/.314240376d0,.947788391d0,1.59768264d0,2.27950708d0
#     ,        ,3.02063703d0,3.8897249d0/
#      Data U/1.01172805d0,-.75197147d0,1.2557727d-2,1.00220082d-2
#     ,        ,-2.42068135d-4,5.00848061d-7/
#      Data S/1.393237d0,.231152406d0,-.155351466d0,6.21836624d-3
#     ,        ,9.19082986d-5,-6.27525958d-7/
#	Data zone,zi/(1.d0,0.D0),(0.d0,1.D0)/
#	data tt/0.5d0,1.5d0,2.5d0,3.5d0,4.5d0,5.5d0,6.5d0,7.5d0,8.5d0,
#     ,        9.5d0,10.5d0,11.5d0,12.5d0,13.5d0,14.5d0/
#	data pipwoeronehalf/0.564189583547756d0/

#C new Region 3
    if dsqrt(X*X+Y*Y)>8.0E0:
        zm1=zone/dcmplx(X,Y)
        zm2=zm1*zm1
        zsum=zone
        zterm=zone
        for i in range(15): #do i=1,15
            zterm=zterm*zm2*tt[i]
            zsum=zsum+zterm
        #end do
        zsum=zsum*zi*zm1*pipwoeronehalf
        #WR=dreal(zsum) # np.real is not processed by Numba
        #WI=dimag(zsum) # np.imag is not processed by Numba
        WR = zsum.real
        WI = zsum.imag
    
    else:
#C
        WR=0.0E0
        WI=0.0E0
        Y1=Y+1.5E0
        Y2=Y1*Y1
        
        if ( (Y>0.85E0) or (dabs(X)<(18.1E0*Y+1.65E0)) ):#GoTo 2
      
            #C
            #C       Region 1
            #C
            #2    Continue
            for I in range(6): #Do 3 I=1,6
                R=X-T[I]
                D=1.0E0/(R*R+Y2)
                D1=Y1*D
                D2=R*D
                R=X+T[I]
                D=1.0E0/(R*R+Y2)
                D3=Y1*D
                D4=R*D
                WR=WR+U[I]*(D1+D3)-S[I]*(D2-D4)
                WI=WI+U[I]*(D2+D4)+S[I]*(D1-D3)
            #3    Continue  
      
        else:
            #C
            #C       Region 2
            #C
            if ( dabs(X)<12.0E0 ): WR=dexp(-X*X)
            Y3=Y+3.0E0
            for I in range(6): #Do 1 I=1,6
                R=X-T[I]
                R2=R*R
                D=1.0E0/(R2+Y2)
                D1=Y1*D
                D2=R*D
                WR=WR+Y*(U[I]*(R*D2-1.5E0*D1)+S[I]*Y3*D2)/(R2+2.25E0)
                R=X+T[I]
                R2=R*R
                D=1.0E0/(R2+Y2)
                D3=Y1*D
                D4=R*D
                WR=WR+Y*(U[I]*(R*D4-1.5E0*D3)-S[I]*Y3*D4)/(R2+2.25E0)
                WI=WI+U[I]*(D2+D4)+S[I]*(D1-D3)
            #1    Continue  
#      Return

    # common part
    return WR, WI
 
#      Return
#      End

@njit(fastmath=FASTMATH)
def HTP_CPF3(X,Y): # => WR,WI
#C-------------------------------------------------
#C "CPF": Complex Probability Function
#C .........................................................
#C         .       Subroutine to Compute the Complex       .
#C         .        Probability Function W(z=X+iY)         .
#C         .     W(z)=exp(-z**2)*Erfc(-i*z) with Y>=0      .
#C         .    Which Appears when Convoluting a Complex   .
#C         .     Lorentzian Profile by a Gaussian Shape    .
#C         .................................................
#C
#C             WR : Real Part of W(z)
#C             WI : Imaginary Part of W(z)
#C
#C This Routine takes into account the region 3 only, i.e. when sqrt(x**2+y**2)>8. 
#C
#C
#C
#C Accessed Files:  None
#C --------------
#C
#C Called Routines: None                               
#C ---------------                                 
#C
#C Called By: 'pCqSDHC'
#C ---------
#C
#C Double Precision Version
#C 
#C-------------------------------------------------
#C      
#      Implicit None
#        Integer I
#	double complex zm1,zm2,zterm,zsum,zone,zi
#      Double Precision X,Y,WR,WI
#      Double Precision TT(15),pipwoeronehalf
#C      
#	Data zone,zi/(1.d0,0.D0),(0.d0,1.D0)/
#	data tt/0.5d0,1.5d0,2.5d0,3.5d0,4.5d0,5.5d0,6.5d0,7.5d0,8.5d0,
#     ,        9.5d0,10.5d0,11.5d0,12.5d0,13.5d0,14.5d0/
#	data pipwoeronehalf/0.564189583547756d0/

#C Region 3
    zm1=zone/dcmplx(X,Y)
    zm2=zm1*zm1
    zsum=zone
    zterm=zone
    for i in range(15): #do i=1,15
        zterm=zterm*zm2*tt[i]
        zsum=zsum+zterm
    #end do
    zsum=zsum*zi*zm1*pipwoeronehalf
    #wr=dreal(zsum) # np.real is not processed by Numba
    #wi=dimag(zsum) # np.imag is not processed by Numba
    wr=zsum.real
    wi=zsum.imag
    return wr, wi
    #return
    #  End

#=====================================    
# CPF APPROXIMATIONS BY V.P. KOCHANOV
#=====================================    

@njit    
def VPKOCHANOV2011a_CPF(x,y):
    """
    Optimized six-term w(x) implementation from the following source:
        Kochanov VP. Efficient approximations of the voigt and rautian-sobelman profiles. 
        Atmos Ocean Opt 2011;24:432-5. doi:10.1134/S1024856011050071.
    """
    z = x+1.0j*y
    frac = ( -0.05211401840823116  +  0.0003456274128399508 * 1.0j ) /       \
           ( -2.202137594489422    +  1.7617409842934335    * 1.0j + z )  +  \
                                                                             \
           (  0.8821478539987956   +  0.3250098013190268    * 1.0j ) /       \
           ( -1.3840051115670835   +  1.815878063693693     * 1.0j + z )  +  \
                                                                             \
           ( -2.4268851379483554   -  2.9002337097791298    * 1.0j ) /       \
           ( -0.6786310070029026   +  1.8211984045812444    * 1.0j + z )  +  \
                                                                             \
           (  0.5609724629394014   +  5.189424779184198     * 1.0j ) /       \
           (  0.002952977671691485 +  1.7784231116089055    * 1.0j + z )  +  \
                                                                             \
           (  1.300047290374259    -  2.142744693364249     * 1.0j ) /       \
           (  0.7245716698265852   +  1.6859739842469321    * 1.0j + z )  +  \
                                                                             \
           ( -0.26417705095694927  +  0.09241240182928095   * 1.0j ) /       \
           (  1.5821736769718555   +  1.5380747276250106    * 1.0j + z )
    return frac.real,frac.imag

@njit
def VPKOCHANOV2011b_CPF(x,y):
    """
    Optimized four-term w(x) implementation from the following source:
        Kochanov VP. Efficient approximations of the voigt and rautian-sobelman profiles. 
        Atmos Ocean Opt 2011;24:432-5. doi:10.1134/S1024856011050071.
    """
    z = x+1.0j*y
    frac = (  0.15269784972008846 - 0.01429059277879871 * 1.0j ) /       \
           ( -1.638566939130925   + 1.390393937133246   * 1.0j + z )  +  \
                                                                         \
           ( -1.022455279362302   - 0.5341298895517378  * 1.0j ) /       \
           ( -0.7327122194675937  + 1.4139283787114099  * 1.0j + z )  +  \
                                                                         \
           (  0.6543564876039407  + 1.5745917089047585  * 1.0j ) /       \
           (  0.10206925308122759 + 1.3554420881163243  * 1.0j + z )  +  \
                                                                         \
           (  0.2156138105591975  - 0.46143197880686115 * 1.0j ) /       \
           (  1.0446775963500718  + 1.2115613850263882  * 1.0j + z )
    return frac.real,frac.imag

@njit    
def VPKOCHANOV2016a_CPF(x,y):
    """
    Optimized w(x) implementation from Eq. (12) of the following source:
        Kochanov VP. Speed-dependent spectral line profile including line narrowing and mixing. 
        J Quant Spectrosc Radiat Transf 2016;177:261-8. doi:10.1016/j.jqsrt.2016.02.014.
    """
    z = x+1.0j*y
    frac = -(  0.018251119415171897 + 0.0011091775583889664 * 1.0j) /       \
            ( -2.39340189207133     + 1.649584324279405     * 1.0j + z ) +  \
                                                                            \
            (  0.6364696342366418   + 0.16903373187925116   * 1.0j) /       \
            ( -1.4278136423151655   + 1.711615300655054     * 1.0j + z ) -  \
                                                                            \
            (  2.6610735659954523   + 2.7191997763898654    * 1.0j) /       \
            ( -0.6383331812421346   + 1.781939574408671     * 1.0j + z ) +  \
                                                                            \
            (  1.4753996346624045   + 6.690524263010301     * 1.0j) /       \
            (  0.08454163207752367  + 1.850037483542174     * 1.0j + z)  +  \
                                                                            \
            (  1.2263526041373907   - 4.573420850868894     * 1.0j) /       \
            (  0.7771177193297836   + 1.91862570385953      * 1.0j + z)  -  \
                                                                            \
            (  0.7459461072848965   - 1.0570578814989129    * 1.0j) /       \
            (  1.4431801794978592   + 1.9775874538601401    * 1.0j + z)  +  \
                                                                            \
            (  0.08704005100135857  - 0.058688080300181494  * 1.0j) /       \
            (  2.1547091847234636   + 1.922349231346897     * 1.0j + z)
    return frac.real,frac.imag
    
@njit
def VPKOCHANOV2016b_CPF(x,y):
    """
    Optimized w(x) implementation from Eq. (13) of the following source:
        Kochanov VP. Speed-dependent spectral line profile including line narrowing and mixing. 
        J Quant Spectrosc Radiat Transf 2016;177:261-8. doi:10.1016/j.jqsrt.2016.02.014.
    """
    z = x+1.0j*y
    frac = 1.0j/np.sqrt(np.pi) * 1./(z-1./2/(z-(1./(z-(3./2/(z-2./(z-5./2/(z-3./z))))))))
    return frac.real,frac.imag
    
@njit
def DUMMY_CPF(x,y):
    return 0.0,0.0
    
# ====================================================================
#             COMPLEX PROBABILITY FUNCTION SELECTORS
# ====================================================================
CPF = hum1_wei  # Schreier   
#CPF3 = hum1_wei  # Schreier

#CPF = DUMMY_CPF # dummy zero function
#CPF3 = DUMMY_CPF # dummy zero function

#CPF = fadf # Abrarov et al. # Faster in raw comparison, but slower in x-section calc !?
#CPF3 = fadf # Abrarov et al.

#CPF = CPF3 

#CPF = HTP_CPF                      # DEFAULT, Tran et al.
CPF3 = HTP_CPF3                    # DEFAULT, Tran et al.

#CPF = VPKOCHANOV2016a_CPF
#CPF3 = VPKOCHANOV2016a_CPF

#CPF = VPKOCHANOV2016b_CPF
#CPF3 = VPKOCHANOV2016b_CPF

#CPF = VPKOCHANOV2011a_CPF
#CPF3 = VPKOCHANOV2011a_CPF

#CPF = VPKOCHANOV2011b_CPF
#CPF3 = VPKOCHANOV2011b_CPF
# ====================================================================

    
@njit(fastmath=FASTMATH)
def qSDV(sg0,GamD,Gam0,Gam2,Shift0,Shift2,sg): # => LS_qSDV_R,LS_qSDV_I
#C-------------------------------------------------
#C	"qSDV": quadratic-Speed-Dependent Voigt
#C	Subroutine to Compute the complex normalized spectral shape of an 
#C	isolated line by the qSDV model following the two references:
#C	[1] Ngo NH, Lisak D, Tran H, Hartmann J-M. An isolated line-shape model
#C	to go beyond the Voigt profile in spectroscopic databases and radiative
#C	transfer codes. J Quant Radiat Transfer 2013;129:89-100.	 	
#C	[2] Tran H, Ngo NH, Hartmann J-M. Efficient computation of some speed-dependent 
#C	isolated line profiles. J Quant Radiat Transfer 2013;129:199-203.
#C
#C	Input/Output Parameters of Routine (Arguments or Common)
#C	---------------------------------
#C	T	    : Temperature in Kelvin (Input).
#C	amM1	: Molar mass of the absorber in g/mol(Input).
#C	sg0		: Unperturbed line position in cm-1 (Input).
#C	GamD	: Doppler HWHM in cm-1 (Input)
#C	Gam0	: Speed-averaged line-width in cm-1 (Input). 	
#C	Gam2	: Speed dependence of the line-width in cm-1 (Input).
#C	Shift0	: Speed-averaged line-shift in cm-1 (Input).
#C	Shift2	: Speed dependence of the line-shift in cm-1 (Input)	 
#C	sg		: Current WaveNumber of the Computation in cm-1 (Input).
#C
#C	Output Quantities (through Common Statements)
#C	-----------------
#C	LS_qSDV_R: Real part of the normalized spectral shape (cm)
#C	LS_qSDV_I: Imaginary part of the normalized spectral shape (cm)
#C
#C	Called Routines: 'CPF'	(Complex Probability Function)
#C	---------------  'CPF3'	(Complex Probability Function for the region 3)
#C
#C	Called By: Main Program
#C	---------
#C
#C	Double Precision Version
#C
#C-------------------------------------------------
#	implicit none
#	 double precision sg0,GamD
#	 double precision Gam0,Gam2,Shift0,Shift2
#	 double precision sg
#	 double precision pi,rpi,cte
#	 double precision xz1,xz2,yz1,yz2,xXb,yXb
#	 double precision wr1,wi1,wr2,wi2,wrb,wib
#	 double precision SZ1,SZ2,DSZ,SZmx,SZmn
#	 double precision LS_qSDV_R,LS_qSDV_I
#	double complex c0,c2,c0t,c2t
#	double complex X,Y,iz,Z1,Z2,csqrtY
#	double complex Aterm,LS_qSDV
#C
#C-------------------------------------------------
#C
    cte=dsqrt(dlog(2.0E0))/GamD
    pi=4.0E0*datan(1.0E0)
    rpi=dsqrt(pi)
    iz=dcmplx(0.0E0,1.0E0)
    #c Calculating the different parameters 
    c0=dcmplx(Gam0,Shift0)
    c2=dcmplx(Gam2,Shift2)
    c0t=(c0-1.5E0*c2)
    c2t=c2
    #C
    if (cdabs(c2t)==0.0E0):	# CONDITION 1
    #c when c2t=0
        Z1=(iz*(sg0-sg)+c0t)*cte
        #xZ1=-dimag(Z1) # np.imag is not processed by Numba
        xZ1=-Z1.imag 
        #yZ1=dreal(Z1) # np.real is not processed by Numba
        yZ1=Z1.real # np.real is not processed by Numba
        WR1,WI1 = CPF(xZ1,yZ1)
        Aterm=rpi*cte*dcmplx(WR1,WI1)
    else:
        X=(iz*(sg0-sg)+c0t)/c2t
        Y=1.0E0/((2.0E0*cte*c2t))**2		
        csqrtY=(Gam2-iz*Shift2)/(2.0E0*cte*(Gam2**2+Shift2**2))
            
        #c calculating Z1 and Z2
        Z1=cdsqrt(X+Y)-csqrtY
        Z2=Z1+2.0E0*csqrtY
        #c calculating the real and imaginary parts of Z1 and Z2
        #xZ1=-dimag(Z1) # np.imag is not processed by Numba
        #yZ1=dreal(Z1) # np.real is not processed by Numba
        #xZ2=-dimag(Z2) # np.imag is not processed by Numba
        #yZ2=dreal(Z2) # np.real is not processed by Numba
        xZ1=-Z1.imag 
        yZ1=Z1.real 
        xZ2=-Z2.imag 
        yZ2=Z2.real 
        #c check if Z1 and Z2 are close to each other
        SZ1=dsqrt(xZ1*xZ1+yZ1*yZ1)
        SZ2=dsqrt(xZ2*xZ2+yZ2*yZ2)
        DSZ=dabs(SZ1-SZ2)
        SZmx=dmax1(SZ1,SZ2)
        SZmn=dmin1(SZ1,SZ2)
        #c when Z1 and Z2 are close to each other, ensure that they are in 
        #c the same interval of CPF 
        if (DSZ<=1.0E0 and SZmx>8.0E0 and SZmn<=8.0E0):
            WR1,WI1 = CPF3(xZ1,yZ1) 
            WR2,WI2 = CPF3(xZ2,yZ2) 
        else:	
            WR1,WI1 = CPF(xZ1,yZ1) 
            WR2,WI2 = CPF(xZ2,yZ2) 

        #c calculating the A term of the profile
        Aterm=rpi*cte*(dcmplx(WR1,WI1)-dcmplx(WR2,WI2))
        #write(15,*)sg,wr1,wr2

        if (cdabs(X)<=3.0E-8*cdabs(Y)): #go to 120 # CONDITION 2
            #c when abs(Y) is much larger than abs(X)
            Z1=(iz*(sg0-sg)+c0t)*cte
            Z2=cdsqrt(X+Y)+csqrtY
            #xZ1=-dimag(Z1) # np.imag is not processed by Numba
            #yZ1=dreal(Z1) # np.real is not processed by Numba
            #xZ2=-dimag(Z2) # np.imag is not processed by Numba
            #yZ2=dreal(Z2) # np.real is not processed by Numba
            xZ1=-Z1.imag
            yZ1=Z1.real 
            xZ2=-Z2.imag
            yZ2=Z2.real 
            WR1,WI1 = CPF(xZ1,yZ1)
            WR2,WI2 = CPF(xZ2,yZ2) 
            Aterm=rpi*cte*(dcmplx(WR1,WI1)-dcmplx(WR2,WI2))
        
        elif (cdabs(Y)<=1.0E-15*cdabs(X)): #go to 140 # CONDITION 3
            #c when abs(X) is much larger than abs(Y)
            if (cdabs(cdsqrt(X))<=4.0E+3): 
                #xXb=-dimag(cdsqrt(X)) # np.imag is not processed by Numba
                #yXb=dreal(cdsqrt(X)) # np.real is not processed by Numba
                xXb=-cdsqrt(X).imag
                yXb=cdsqrt(X).real 
                WRb,WIb = CPF(xXb,yXb) 
                Aterm=(2.0E0*rpi/c2t)*(1.0E0/rpi-cdsqrt(X)*dcmplx(WRb,WIb))
            #cc and when abs(X) is much larger than 1
            else:
                Aterm=(1.0E0/c2t)*(1.0E0/X-1.5E0/(X**2))
    #c
    #10    continue  
    #c
    LS_qSDV=(1.0E0/pi)*Aterm
    
    #LS_qSDV_R=dreal(LS_qSDV) # np.real is not processed by Numba
    #LS_qSDV_I=dimag(LS_qSDV) # np.imag is not processed by Numba
    LS_qSDV_R=LS_qSDV.real 
    LS_qSDV_I=LS_qSDV.imag 
    
    return LS_qSDV_R, LS_qSDV_I
   
#      Return
#      End Subroutine qSDV    

@njit(fastmath=FASTMATH)
def PROFILE_SDVOIGT(sg0, GamD, Gam0, Gam2, Shift0, Shift2, sg):
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
    LS_qSDV_R = np.zeros(n)
    LS_qSDV_I = np.zeros(n)
    for i in range(n):
        LS_qSDV_R[i],LS_qSDV_I[i] = qSDV(sg0,GamD,Gam0,Gam2,Shift0,Shift2,sg[i])
    return LS_qSDV_R, LS_qSDV_I
    
# OTHER MORE SIMPLE PROFILES

@njit(fastmath=FASTMATH)
def PROFILE_LORENTZ(sg0,Gam0,sg):
    """
    # Lorentz profile.
    # Input parameters:
    #   sg0: Unperturbed line position in cm-1 (Input).
    #   Gam0: Speed-averaged line-width in cm-1 (Input).       
    #   sg: Current WaveNumber of the Computation in cm-1 (Input).
    """
    return Gam0/(np.pi*(Gam0**2+(sg-sg0)**2))

cSqrtLn2divSqrtPi = 0.469718639319144059835
cLn2 = 0.6931471805599
    
@njit(fastmath=FASTMATH)    
def PROFILE_DOPPLER(sg0,GamD,sg):
    """
    # Doppler profile.
    # Input parameters:
    #   sg0: Unperturbed line position in cm-1 (Input).
    #   GamD: Doppler HWHM in cm-1 (Input)
    #   sg: Current WaveNumber of the Computation in cm-1 (Input).
    """
    return cSqrtLn2divSqrtPi*np.exp(-cLn2*((sg-sg0)/GamD)**2)/GamD
