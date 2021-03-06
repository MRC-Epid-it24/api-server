package uk.ac.ncl.openlab.intake24.sql.tools.food.nutrients

import uk.ac.ncl.openlab.intake24.nutrientsndns.CsvNutrientTableParser

object ArabicCompositionTableMappings {

  import CsvNutrientTableParser.{excelColumnToOffset => col}

  val USDA = Map(1l -> col("D"),
    8l -> col("C"),
    11l -> col("E"),
    13l -> col("H"),
    17l -> col("I"),
    22l -> col("J"),
    49l -> col("F"),
    50l -> col("AS"),
    51l -> col("AT"),
    52l -> col("AU"),
    59l -> col("AV"),
    114l -> col("AI"),
    116l -> col("AJ"),
    117l -> col("AK"),
    119l -> col("AL"),
    121l -> col("AH"),
    122l -> col("AP"),
    123l -> col("V"),
    124l -> col("W"),
    125l -> col("X"),
    129l -> col("U"),
    130l -> col("AO"),
    132l -> col("Z"),
    133l -> col("AF"),
    134l -> col("AA"),
    135l -> col("AC"),
    136l -> col("Y"),
    138l -> col("P"),
    139l -> col("O"),
    140l -> col("K"),
    141l -> col("M"),
    142l -> col("N"),
    143l -> col("L"),
    146l -> col("R"),
    147l -> col("Q"),
    151l -> col("S"),
    152l -> col("T"),
    157l -> col("G"),
    162l -> col("AD"),
    163l -> col("AB"))


  val UAE = Map(1l -> col("C"),
    8l -> col("B"),
    11l -> col("D"),
    13l -> col("F"),
    17l -> col("G"),
    22l -> col("H"),
    49l -> col("E"),
    50l -> col("AA"),
    51l -> col("AB"),
    52l -> col("AC"),
    58l -> col("AD"),
    59l -> col("AE"),
    121l -> col("W"),
    122l -> col("Y"),
    124l -> col("R"),
    125l -> col("S"),
    129l -> col("P"),
    131l -> col("X"),
    132l -> col("T"),
    133l -> col("V"),
    134l -> col("U"),
    138l -> col("N"),
    139l -> col("L"),
    140l -> col("I"),
    141l -> col("K"),
    142l -> col("L"),
    143l -> col("J"),
    147l -> col("O"),
    158l -> col("AF"),
    159l -> col("F"))


  val Kuwait = Map(
    1l -> col("E"),
    2l -> col("F"),
    8l -> col("D"),
    11l -> col("G"),
    13l -> col("I"),
    17l -> col("K"),
    45l -> col("J"),
    49l -> col("H"),
    50l -> col("AP"),
    51l -> col("BH"),
    52l -> col("BT"),
    59l -> col("CE"),
    121l -> col("AM"),
    122l -> col("CY"),
    123l -> col("AF"),
    124l -> col("AG"),
    125l -> col("AH"),
    127l -> col("CF"),
    130l -> col("CX"),
    132l -> col("AJ"),
    133l -> col("AL"),
    136l -> col("AI"),
    138l -> col("P"),
    139l -> col("Q"),
    140l -> col("R"),
    141l -> col("T"),
    142l -> col("S"),
    143l -> col("U"),
    146l -> col("V"),
    147l -> col("W"),
    149l -> col("AB"),
    151l -> col("X"),
    152l -> col("AC"),
    157l -> col("N"),
    181l -> col("AE"),
    183l -> col("Z"),
    184l -> col("AD"),
    211l -> col("CH"),
    212l -> col("CI"),
    213l -> col("CJ"),
    214l -> col("CK"),
    215l -> col("CL"),
    216l -> col("CM"),
    217l -> col("CN"),
    218l -> col("CG"),
    219l -> col("CO"),
    220l -> col("CP"),
    221l -> col("CQ"),
    222l -> col("CR"),
    223l -> col("CS"),
    224l -> col("CT"),
    225l -> col("CU"),
    226l -> col("CW"),
    227l -> col("CV"))

  val Bahrain = Map(1l -> col("E"),
    2l -> col("F"),
    8l -> col("D"),
    11l -> col("G"),
    13l -> col("I"),
    17l -> col("K"),
    18l -> col("M"),
    19l -> col("L"),
    45l -> col("J"),
    49l -> col("H"),
    50l -> col("AP"),
    51l -> col("BH"),
    52l -> col("BT"),
    59l -> col("CE"),
    70l -> col("AQ"),
    71l -> col("AR"),
    72l -> col("AS"),
    73l -> col("AT"),
    74l -> col("AU"),
    75l -> col("AW"),
    76l -> col("AX"),
    77l -> col("AY"),
    78l -> col("AZ"),
    79l -> col("BA"),
    80l -> col("BC"),
    81l -> col("BE"),
    82l -> col("BG"),
    85l -> col("BJ"),
    86l -> col("BM"),
    87l -> col("BL"),
    90l -> col("BP"),
    91l -> col("BO"),
    120l -> col("AM"),
    122l -> col("AO"),
    123l -> col("AF"),
    124l -> col("AG"),
    125l -> col("AH"),
    126l -> col("CF"),
    130l -> col("AN"),
    132l -> col("AJ"),
    133l -> col("AL"),
    138l -> col("P"),
    139l -> col("Q"),
    140l -> col("R"),
    141l -> col("T"),
    142l -> col("S"),
    143l -> col("U"),
    146l -> col("V"),
    147l -> col("W"),
    148l -> col("AA"),
    149l -> col("AB"),
    151l -> col("X"),
    152l -> col("AC"),
    157l -> col("N"),
    181l -> col("AE"),
    184l -> col("AD"),
    211l -> col("CH"),
    212l -> col("CI"),
    213l -> col("CJ"),
    214l -> col("CK"),
    215l -> col("CL"),
    216l -> col("CM"),
    217l -> col("CN"),
    218l -> col("CG"),
    219l -> col("CO"),
    220l -> col("CP"),
    221l -> col("CQ"),
    222l -> col("CR"),
    223l -> col("CS"),
    224l -> col("CT"),
    225l -> col("CU"),
    226l -> col("CW"),
    227l -> col("CV"))

  val Oman = Map(1l -> col("E"),
    2l -> col("F"),
    8l -> col("D"),
    11l -> col("G"),
    13l -> col("I"),
    17l -> col("K"),
    18l -> col("M"),
    19l -> col("L"),
    45l -> col("J"),
    49l -> col("H"),
    50l -> col("AP"),
    51l -> col("BH"),
    52l -> col("BT"),
    59l -> col("CE"),
    70l -> col("AQ"),
    71l -> col("AR"),
    72l -> col("AS"),
    73l -> col("AT"),
    74l -> col("AU"),
    75l -> col("AW"),
    76l -> col("AX"),
    77l -> col("AY"),
    78l -> col("AZ"),
    79l -> col("BA"),
    80l -> col("BC"),
    81l -> col("BE"),
    82l -> col("BG"),
    85l -> col("BJ"),
    86l -> col("BM"),
    87l -> col("BL"),
    90l -> col("BP"),
    91l -> col("BO"),
    121l -> col("AM"),
    123l -> col("AF"),
    124l -> col("AG"),
    125l -> col("AH"),
    127l -> col("CF"),
    132l -> col("AJ"),
    133l -> col("AL"),
    136l -> col("AI"),
    138l -> col("P"),
    139l -> col("Q"),
    140l -> col("R"),
    141l -> col("T"),
    142l -> col("S"),
    143l -> col("U"),
    146l -> col("V"),
    147l -> col("W"),
    149l -> col("AB"),
    151l -> col("X"),
    157l -> col("N"),
    181l -> col("AE"),
    183l -> col("Z"),
    184l -> col("AD"),
    211l -> col("CH"),
    212l -> col("CI"),
    213l -> col("CJ"),
    214l -> col("CK"),
    215l -> col("CL"),
    216l -> col("CM"),
    217l -> col("CN"),
    218l -> col("CG"),
    219l -> col("CO"),
    220l -> col("CP"),
    221l -> col("CQ"),
    222l -> col("CR"),
    223l -> col("CS"),
    224l -> col("CT"),
    225l -> col("CU"),
    226l -> col("CW"),
    227l -> col("CV"))

  val Qatar = Map(1l -> col("E"),
    2l -> col("F"),
    8l -> col("D"),
    11l -> col("G"),
    13l -> col("I"),
    17l -> col("K"),
    18l -> col("M"),
    19l -> col("L"),
    45l -> col("J"),
    49l -> col("H"),
    50l -> col("AP"),
    51l -> col("BH"),
    52l -> col("BT"),
    59l -> col("CE"),
    70l -> col("AQ"),
    71l -> col("AR"),
    72l -> col("AS"),
    73l -> col("AT"),
    74l -> col("AU"),
    75l -> col("AW"),
    76l -> col("AX"),
    77l -> col("AY"),
    78l -> col("AZ"),
    79l -> col("BA"),
    80l -> col("BC"),
    81l -> col("BE"),
    82l -> col("BG"),
    85l -> col("BJ"),
    86l -> col("BM"),
    87l -> col("BL"),
    90l -> col("BP"),
    91l -> col("BO"),
    121l -> col("AM"),
    123l -> col("AF"),
    124l -> col("AG"),
    125l -> col("AH"),
    127l -> col("CF"),
    132l -> col("AJ"),
    136l -> col("AI"),
    138l -> col("P"),
    139l -> col("Q"),
    140l -> col("R"),
    141l -> col("T"),
    142l -> col("S"),
    143l -> col("U"),
    146l -> col("V"),
    147l -> col("W"),
    149l -> col("AB"),
    151l -> col("X"),
    152l -> col("AC"),
    157l -> col("N"),
    181l -> col("AE"),
    183l -> col("Z"),
    184l -> col("AD"),
    211l -> col("CH"),
    212l -> col("CI"),
    213l -> col("CJ"),
    214l -> col("CK"),
    215l -> col("CL"),
    216l -> col("CM"),
    217l -> col("CN"),
    218l -> col("CG"),
    219l -> col("CO"),
    220l -> col("CP"),
    221l -> col("CQ"),
    222l -> col("CR"),
    223l -> col("CS"),
    224l -> col("CT"),
    225l -> col("CU"),
    226l -> col("CW"),
    227l -> col("CV"))

  val Saudi = Map(1l -> col("E"),
    2l -> col("F"),
    8l -> col("D"),
    11l -> col("G"),
    13l -> col("I"),
    17l -> col("K"),
    18l -> col("M"),
    19l -> col("L"),
    45l -> col("J"),
    49l -> col("H"),
    50l -> col("AP"),
    51l -> col("BH"),
    52l -> col("BT"),
    59l -> col("CE"),
    70l -> col("AQ"),
    71l -> col("AR"),
    72l -> col("AS"),
    73l -> col("AT"),
    74l -> col("AU"),
    75l -> col("AW"),
    76l -> col("AX"),
    77l -> col("AY"),
    78l -> col("AZ"),
    79l -> col("BA"),
    80l -> col("BC"),
    81l -> col("BE"),
    82l -> col("BG"),
    85l -> col("BJ"),
    86l -> col("BM"),
    87l -> col("BL"),
    90l -> col("BP"),
    91l -> col("BO"),
    120l -> col("AM"),
    123l -> col("AF"),
    124l -> col("AG"),
    125l -> col("AH"),
    127l -> col("CF"),
    132l -> col("AJ"),
    133l -> col("AL"),
    136l -> col("AI"),
    138l -> col("P"),
    139l -> col("Q"),
    140l -> col("R"),
    141l -> col("T"),
    142l -> col("S"),
    143l -> col("U"),
    146l -> col("V"),
    147l -> col("W"),
    149l -> col("AB"),
    151l -> col("X"),
    152l -> col("AC"),
    157l -> col("N"),
    181l -> col("AE"),
    183l -> col("Z"),
    184l -> col("AD"),
    211l -> col("CH"),
    212l -> col("CI"),
    213l -> col("CJ"),
    214l -> col("CK"),
    215l -> col("CL"),
    216l -> col("CM"),
    217l -> col("CN"),
    218l -> col("CG"),
    219l -> col("CO"),
    220l -> col("CP"),
    221l -> col("CQ"),
    222l -> col("CR"),
    223l -> col("CS"),
    224l -> col("CT"),
    225l -> col("CU"),
    226l -> col("CW"),
    227l -> col("CV"))

  val Recipes = Map(1l -> col("F"),
    8l -> col("E"),
    11l -> col("G"),
    13l -> col("J"),
    17l -> col("K"),
    22l -> col("L"),
    49l -> col("H"),
    50l -> col("AU"),
    51l -> col("AV"),
    52l -> col("AW"),
    59l -> col("AX"),
    114l -> col("AK"),
    116l -> col("AL"),
    117l -> col("AM"),
    119l -> col("AN"),
    121l -> col("AH"),
    122l -> col("AR"),
    123l -> col("X"),
    124l -> col("Y"),
    125l -> col("Z"),
    129l -> col("W"),
    130l -> col("AQ"),
    132l -> col("AB"),
    133l -> col("AH"),
    134l -> col("AC"),
    135l -> col("AE"),
    136l -> col("AA"),
    138l -> col("R"),
    139l -> col("Q"),
    140l -> col("M"),
    141l -> col("O"),
    142l -> col("P"),
    143l -> col("N"),
    146l -> col("T"),
    147l -> col("S"),
    151l -> col("U"),
    152l -> col("V"),
    157l -> col("I"),
    162l -> col("AF"),
    230l -> col("AD"),
    241l -> col("AI"))

}
