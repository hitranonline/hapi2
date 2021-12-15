# based on Christian Hill's code
from pyparsing import Word, Group, Optional, OneOrMore, ParseException,\
                      Literal, StringEnd

# the elements, as a tuple of (atomic number, atomic symbol, and name):
elements = [
(1, 'D', 'Deuterium'),  # NB Deuterium has its own, special entry
(1, 'H', 'Hydrogen'),
(2, 'He', 'Helium'),
(3, 'Li', 'Lithium'),
(4, 'Be', 'Beryllium'),
(5, 'B', 'Boron'),
(6, 'C', 'Carbon'),
(7, 'N', 'Nitrogen'),
(8, 'O', 'Oxygen'),
(9, 'F', 'Fluorine'),
(10, 'Ne', 'Neon'),
(11, 'Na', 'Sodium'),
(12, 'Mg', 'Magnesium'),
(13, 'Al', 'Aluminum'),
(14, 'Si', 'Silicon'),
(15, 'P', 'Phosphorus'),
(16, 'S', 'Sulfur'),
(17, 'Cl', 'Chlorine'),
(18, 'Ar', 'Argon'),
(19, 'K', 'Potassium'),
(20, 'Ca', 'Calcium'),
(21, 'Sc', 'Scandium'),
(22, 'Ti', 'Titanium'),
(23, 'V', 'Vanadium'),
(24, 'Cr', 'Chromium'),
(25, 'Mn', 'Manganese'),
(26, 'Fe', 'Iron'),
(27, 'Co', 'Cobalt'),
(28, 'Ni', 'Nickel'),
(29, 'Cu', 'Copper'),
(30, 'Zn', 'Zinc'),
(31, 'Ga', 'Gallium'),
(32, 'Ge', 'Germanium'),
(33, 'As', 'Arsenic'),
(34, 'Se', 'Selenium'),
(35, 'Br', 'Bromine'),
(36, 'Kr', 'Krypton'),
(37, 'Rb', 'Rubidium'),
(38, 'Sr', 'Strontium'),
(39, 'Y', 'Yttrium'),
(40, 'Zr', 'Zirconium'),
(41, 'Nb', 'Niobium'),
(42, 'Mo', 'Molybdenum'),
(43, 'Tc', 'Technetium'),
(44, 'Ru', 'Ruthenium'),
(45, 'Rh', 'Rhodium'),
(46, 'Pd', 'Palladium'),
(47, 'Ag', 'Silver'),
(48, 'Cd', 'Cadmium'),
(49, 'In', 'Indium'),
(50, 'Sn', 'Tin'),
(51, 'Sb', 'Antimony'),
(52, 'Te', 'Tellurium'),
(53, 'I', 'Iodine'),
(54, 'Xe', 'Xenon'),
(55, 'Cs', 'Cesium'),
(56, 'Ba', 'Barium'),
(57, 'La', 'Lanthanum'),
(58, 'Ce', 'Cerium'),
(59, 'Pr', 'Praseodymium'),
(60, 'Nd', 'Neodymium'),
(61, 'Pm', 'Promethium'),
(62, 'Sm', 'Samarium'),
(63, 'Eu', 'Europium'),
(64, 'Gd', 'Gadolinium'),
(65, 'Tb', 'Terbium'),
(66, 'Dy', 'Dysprosium'),
(67, 'Ho', 'Holmium'),
(68, 'Er', 'Erbium'),
(69, 'Tm', 'Thulium'),
(70, 'Yb', 'Ytterbium'),
(71, 'Lu', 'Lutetium'),
(72, 'Hf', 'Hafnium'),
(73, 'Ta', 'Tantalum'),
(74, 'W', 'Tungsten'),
(75, 'Re', 'Rhenium'),
(76, 'Os', 'Osmium'),
(77, 'Ir', 'Iridium'),
(78, 'Pt', 'Platinum'),
(79, 'Au', 'Gold'),
(80, 'Hg', 'Mercury'),
(81, 'Tl', 'Thallium'),
(82, 'Pb', 'Lead'),
(83, 'Bi', 'Bismuth'),
(84, 'Po', 'Polonium'),
(85, 'At', 'Astatine'),
(86, 'Rn', 'Radon'),
(87, 'Fr', 'Francium'),
(88, 'Ra', 'Radium'),
(89, 'Ac', 'Actinium'),
(90, 'Th', 'Thorium'),
(91, 'Pa', 'Protactinium'),
(92, 'U', 'Uranium'),
(93, 'Np', 'Neptunium'),
(94, 'Pu', 'Plutonium'),
(95, 'Am', 'Americium'),
(96, 'Cm', 'Curium'),
(97, 'Bk', 'Berkelium'),
(98, 'Cf', 'Californium'),
(99, 'Es', 'Einsteinium'),
(100, 'Fm', 'Fermium'),
(101, 'Md', 'Mendelevium'),
(102, 'No', 'Nobelium'),
(103, 'Lr', 'Lawrencium'),
(104, 'Rf', 'Rutherfordium'),
(105, 'Db', 'Dubnium'),
(106, 'Sg', 'Seaborgium'),
(107, 'Bh', 'Bohrium'),
(108, 'Hs', 'Hassium'),
(109, 'Mt', 'Meitnerium'),
(110, 'Ds', 'Darmstadtium'),
(111, 'Rg', 'Roentgenium'),
(112, 'Cn', 'Copernicium'),
]

# a list of element symbols, such that 0: D, 1: H, 2: He, 3: Li, etc.
element_symbols = [element[1] for element in elements]
# element names, keyed by symbol
element_names = {}
# element atomic numbers, keyed by symbol
atomic_numbers = {}
for element in elements:
    symbol = element[1]
    element_names[symbol] = element[2]
    atomic_numbers[symbol] = element[0]

caps = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
lowers = caps.lower()
digits = '0123456789'
element = Word(caps, lowers)
integer = Word(digits)
elementRef = Group(element + Optional(integer, default='1'))
chemicalFormula = OneOrMore(elementRef)
plusminus = Literal('+') | Literal('-')
charge = Group(plusminus + Optional(integer, default='1'))
chargedChemicalFormula = Group(chemicalFormula) + Optional(charge)\
                            + StringEnd()

class FormulaError(Exception):
    def __init__(self, error_str):
        self.error_str = error_str
    def __str__(self):
        return self.error_str

def get_stoichiometric_formula(formula):
    """
    Get the stoichiometric formula, in canonical form (increasing
    atomic mass) from a given formula string.
    e.g. 'CH3OH' -> 'H4CO'
    """
 
    elms = {}
    try:
        chargedformulaData = chargedChemicalFormula.parseString(formula)
    except ParseException:
        raise FormulaError("Invalid formula syntax: %s" % formula)
    formulaData = chargedformulaData[0]
    
    # parse the charge part of the formula, if present:
    charge_string = ''
    if len(chargedformulaData) == 2:
        charge_sign, charge_value = chargedformulaData[1]
        charge_string = charge_sign
        if charge_value != '1':
            charge_string += charge_value

    for symbol, stoich in formulaData:
        try:
            element_name = element_names[symbol]
        except KeyError:
            raise FormulaError("Invalid formula: %s. Unknown element symbol"\
                               " %s" % (formula, symbol))
        atomic_number = atomic_numbers[symbol]
        if atomic_number in elms.keys():
            elms[atomic_number] += int(stoich)
        else:
            elms[atomic_number] = int(stoich)
    elm_strs = []
    for atomic_number in sorted(elms.keys()):
        if elms[atomic_number]>1:
            elm_strs.append('%s%d' % (element_symbols[atomic_number],
                                      elms[atomic_number]))
        else:
            elm_strs.append(element_symbols[atomic_number])
    # finally, add on the charge string, e.g. '', '-', '+2', ...
    elm_strs.append(charge_string)
    return ''.join(elm_strs)
    
# temporary code for the html formula 
index = lambda i: '<sub>%s</sub>'%int(i) if int(i)>1 else ''
get_formula_html = lambda formula: ''.join(['%s%s'%(e[0],index(e[1])) for e in chargedChemicalFormula.parseString(formula)[0]])

parse_formula = lambda formula: chargedChemicalFormula.parseString(formula)[0]

WEIGHTS = {
    'C':12.011,'H':1.00794,'O':15.9994,'N':14.0067,'F':18.998403,'Cl':35.453,'S':32.066,'Br':79.904,
    'I':126.9045,'As':74.9216,'B':10.811,'P':30.97376,'Si':28.0855,'Ge':72.59,'Ti':47.88,'Tu':183.85,
    'Ni':58.69,'He':4.002602,'Ar':39.948}	# Tu=W=Wolfram
molweight = lambda formula: sum([int(n)*WEIGHTS[atom] for atom,n in chargedChemicalFormula.parseString(formula)[0]])
natoms = lambda formula: sum([int(n) for atom,n in chargedChemicalFormula.parseString(formula)[0]])
atoms = lambda formula: {e[0]:int(e[1]) for e in parse_formula(formula)}
