import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, substring}

object DataFrame {

  def main(args: Array[String]): Unit = {
    //fichier code Postaux.csv
    // Le fichier contient 39193 lignes et 6 colonnes : Code_commune_INSEE, Nom_commune, Code_postal, Libelle_acheminement, Ligne_5, coordonnees_gps
    //Combien de colonnes contient-il ?
    //Comment ces colonnes sont-elles distribuées ?
    //6 colonnes : Code_commune_INSEE, Nom_commune, Code_postal, Libelle_acheminement, Ligne_5, coordonnees_gps
    //Quelle sont les types des colonnes du fichier ?
      //Code_commune_INSEE : String,
      // Nom_commune : String,
      // Code_postal : String,
      // Libelle_acheminement : String,
      // Ligne_5 : String nullable,
      // coordonnees_gps : tuple double nullable

    val spark = SparkSession.builder
      .appName("Spark df tp")
      .master("local[*]")
      .getOrCreate()

    // Lisez le fichier des codes postaux avec Spark et affichez son contenu dans la console.

    val filePath= "src/main/ressources/codesPostaux.csv"

    val df = spark.read
      .option("header", "true")
      .option("sep", ";")
      .option("inferSchema", "true")
      .csv(filePath)

    df.show()

    //Quel est le schéma du fichier ?
    println("Schema du fichier :")
    df.printSchema()
    //tout les attribut sont au format string nullable

    //Affichez le nombre de communes.
    val res2=df.count()
    println(s"Nombre de communes : ${res2}")

    //Affichez le nombre de communes qui possèdent l’attribut Ligne_5

    val res3=df.filter("Ligne_5 is not null").count()
    println(s"Nombre de communes qui possèdent l'attribut Ligne_5 : ${res3}")

    //Ajoutez aux données une colonne contenant le numéro de département de la commune.
    println("Ajout de la colonne num_departement :")
    val dfWithDept= df.withColumn("departement", substring(col("Code_commune_INSEE"), 1, 2))
    dfWithDept.show()


    //Ecrivez le résultat dans un nouveau fichier CSV nommé “commune_et_departement.csv”,
    //ayant pour colonne
    // Code_commune_INSEE, Nom_commune, Code_postal, departement, ordonné par code postal.
    println("Ecriture du fichier commune_et_departement.csv :")
    dfWithDept.select("Code_commune_INSEE", "Nom_commune", "Code_postal", "departement")
      .orderBy("Code_postal")
      .write
      .option("header", "true")
      .option("sep", ";")
      .csv("src/main/ressources/commune_et_departement.csv")




  }


}
