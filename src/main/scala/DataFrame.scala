import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._

object DataFrameExample {

  def main(args: Array[String]): Unit = {
    // Initialisation de la session Spark
    val spark = SparkSession.builder()
      .appName("Codes Postaux")
      .config("spark.master", "local")
      .getOrCreate()

    // Chemin du fichier CSV
    val csvFilePath = "src/main/ressources/codesPostaux.csv"

    // Chargement du fichier CSV
    val df = spark.read
      .option("header", "true") // Utilise la première ligne comme en-têtes de colonne
      .option("inferSchema", "true")
      .option("sep",";")// Infère les types de colonnes
      .csv(csvFilePath)

    // Affichage des informations de base sur le DataFrame
    df.printSchema() // Imprime le schéma pour voir les types de colonnes
    df.show() // Affiche les premières lignes pour avoir un aperçu des données

    // Obtenir le nombre de colonnes
    val numberOfColumns = df.columns.length
    println(s"Nombre de colonnes: $numberOfColumns")

    //Quel est le schéma du fichier ?
    df.printSchema() // Imprime le schéma pour voir les types de colonnes
    //Affichez le nombre de communes.
    val nombreCommunes = df.count()
    println(s"Nombre de communes: $nombreCommunes")
    //Affichez le nombre de communes qui possèdent l’attribut Ligne_5
    val nb_commune_ligne_5 = df
      .filter("Ligne_5 is not null")
      .count()
    println(s"Nombre de communes avec Ligne_5: $nb_commune_ligne_5")
    //Ajoutez aux données une colonne contenant le numéro de département de la commune.
    val df_new = df.withColumn("departement", substring(col("Code_commune_INSEE"), 1, 2))
    df_new.show()
    //Ecrivez le résultat dans un nouveau fichier CSV nommé “commune_et_departement.csv”, ayant pour colonne Code_commune_INSEE, Nom_commune, Code_postal, departement, ordonné par code postal.
    df_new
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("src/main/ressources/commune_et_departement.csv")
    //Affichez les communes du département de l’Aisne.
    println("Communes de l'Aisne :")
    val communesAisne = df_new.filter("departement = 02")
    communesAisne.show(false)
    //Quel est le département avec le plus de communes ?
    val comptageParDepartement = df_new.groupBy("departement")
      .agg(count("*").as("nombre_de_communes"))
      .orderBy(desc("nombre_de_communes"))
    val departementPlusDeCommunes = comptageParDepartement.first()
    println(s"Le département avec le plus de communes est : ${departementPlusDeCommunes.getAs[String]("departement")} avec ${departementPlusDeCommunes.getAs[Long]("nombre_de_communes")} communes.")

  }
}
