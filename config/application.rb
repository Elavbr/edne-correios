# encoding: utf-8

require "./config/database.rb"

class Application
  def self.migrate_up
    migrate_up!
  end

  def self.migrate_down
    migrate_down!
  end

  def self.import_from_log
    MODEL_CLASSES.each do |model_class|
      puts "\n[#{model_class}] importing from log"
      model_class.import_from_log
    end
  end

  def self.import_from_delta
    MODEL_CLASSES.each do |model_class|
      puts "\n[#{model_class}] importing from delta"
      model_class.import_from_delta
    end
  end

  def self.dump
    system "echo '.dump ceps' | sqlite3 ./db/database.sqlite3 > db/ceps.sql"
  end

  def self.join
    DataMapper.repository(:default).adapter.execute "DELETE FROM ceps"

    query = "
      INSERT INTO ceps
      SELECT DISTINCT RS.CEP AS CEP,
              RS.ENDERECO AS ENDERECO,
              RS.BAIRRO AS BAIRRO,
              RS.CIDADE AS CIDADE,
              RS.UF AS ESTADO,
              CASE RS.UF
              WHEN 'AC' THEN 'Acre'
              WHEN 'AL' THEN 'Alagoas'
              WHEN 'AP' THEN 'Amapá'
              WHEN 'AM' THEN 'Amazonas'
              WHEN 'BA' THEN 'Bahia'
              WHEN 'CE' THEN 'Ceará'
              WHEN 'DF' THEN 'Distrito Federal'
              WHEN 'ES' THEN 'Espírito Santo'
              WHEN 'GO' THEN 'Goiás'
              WHEN 'MA' THEN 'Maranhão'
              WHEN 'MT' THEN 'Mato Grosso'
              WHEN 'MS' THEN 'Mato Grosso do Sul'
              WHEN 'MG' THEN 'Minas Gerais'
              WHEN 'PA' THEN 'Pará'
              WHEN 'PB' THEN 'Paraíba'
              WHEN 'PR' THEN 'Paraná'
              WHEN 'PE' THEN 'Pernambuco'
              WHEN 'PI' THEN 'Piauí'
              WHEN 'RJ' THEN 'Rio de Janeiro'
              WHEN 'RN' THEN 'Rio Grande do Norte'
              WHEN 'RS' THEN 'Rio Grande do Sul'
              WHEN 'RO' THEN 'Rondônia'
              WHEN 'RR' THEN 'Rorâima'
              WHEN 'SC' THEN 'Santa Catarina'
              WHEN 'SP' THEN 'São Paulo'
              WHEN 'SE' THEN 'Sergipe'
              WHEN 'TO' THEN 'Tocantins'
              END AS NOME_ESTADO
      FROM (
                SELECT  ifnull(lg.CEP, loc.CEP) AS CEP,
                        CASE lg.LOG_STA_TLO
                        WHEN 'S' THEN
                          CONCAT (IFNULL(lg.TLO_TX, ''),' ',IFNULL(lg.LOG_NO, ''))
                        ELSE
                          ifnull(lg.LOG_NO, '')
                        END ENDERECO,
                        ifnull(bai.BAI_NO, '')   AS BAIRRO,
                        loc.LOC_NO               AS CIDADE,
                        loc.UFE_SG               AS UF
                FROM localidades loc
                LEFT JOIN logradouros lg ON lg.LOC_NU = loc.LOC_NU
                LEFT JOIN bairros bai     ON lg.BAI_NU_INI = bai.BAI_NU

                UNION ALL

                SELECT  uni.CEP                  AS CEP,
                        uni.UOP_ENDERECO         AS ENDERECO,
                        ifnull(bai.BAI_NO, '')   AS BAIRRO,
                        loc.LOC_NO               AS CIDADE,
                        uni.UFE_SG               AS UF
                FROM unidade_operacionals uni
                LEFT JOIN bairros bai     ON uni.BAI_NU = bai.BAI_NU
                LEFT JOIN localidades loc ON bai.LOC_NU = loc.LOC_NU

                UNION ALL

                SELECT  gra.CEP                  AS CEP,
                        gra.GRU_ENDERECO         AS ENDERECO,
                        ifnull(bai.BAI_NO, '')   AS BAIRRO,
                        loc.LOC_NO               AS CIDADE,
                        gra.UFE_SG               AS UF
                FROM grande_usuarios gra
                LEFT JOIN bairros bai     ON gra.BAI_NU = bai.BAI_NU
                LEFT JOIN localidades loc ON bai.LOC_NU = loc.LOC_NU
      ) RS
      WHERE RS.CEP <> ''"

    DataMapper.repository(:default).adapter.execute query
  end
end
