# Corroborar entre dimes y distri si los descuentos por marcas no son iguales
# Tomar la info de las dbs y migrarlas local
# pandas.read_sql

from functions import connectionDimesPPAL, connectionDistriPPAL, localCon
import pandas as pd


def updateDB():
    try:
        condist = connectionDistriPPAL()
        condim = connectionDimesPPAL()
        conLocal = localCon()

        distri = pd.read_sql("SELECT codigocliente, codigomarca, porcentajedescuento FROM DESCUENTOCLIENTESMARCAS", condist)
        dimes = pd.read_sql("SELECT codigocliente, codigomarca, porcentajedescuento FROM DESCUENTOCLIENTESMARCAS", condim)
        marcas = pd.read_sql("SELECT codigomarca, descripcion FROM MARCAS where muestraweb = 1", condist)


        distri.to_sql("DescCliDistri", con=conLocal, if_exists="replace", index=False)
        dimes.to_sql("DescCliDimes", con=conLocal, if_exists="replace", index=False )
        marcas.to_sql("Marcas", con=conLocal, if_exists="replace", index=False)

        print('todas las tablas fueron actualizadas correctamente')
        return 1
    except Exception as e:
        print('Fallo la act')
        return 0


def main():
    conLocal = localCon()
    distriDescData = pd.read_sql('SELECT * FROM DescCliDistri', con=conLocal)
    dimesDescData = pd.read_sql('SELECT * FROM DescCliDimes', con=conLocal)
    distriDescData = distriDescData.dropna()
    dimesDescData = dimesDescData.dropna()

    data = pd.DataFrame(columns=["CLIENTEID","CODMARCA","DESCDISTRI","DESCDIMES"])

    try:

        for i in range(len(distriDescData)):
            codigocliente = distriDescData.loc[i, 'CODIGOCLIENTE'] # 06689
            codigomarca = distriDescData.loc[i, 'CODIGOMARCA'] # 015
            porcentajecliente = distriDescData.loc[i, 'PORCENTAJEDESCUENTO']
            

                                            # 06689             == 06689 and                    015 ==              015    
            dimesrow = dimesDescData.loc[(dimesDescData['CODIGOCLIENTE'] == codigocliente) & (dimesDescData['CODIGOMARCA'] == codigomarca)] 
            if dimesrow.empty:
                data.loc[len(data)] = {
                    "CLIENTEID": codigocliente,
                    "CODMARCA": codigomarca,
                    "DESCDIMES": 0,
                    "DESCDISTRI": porcentajecliente 
                }
                #print('No existe en dimes pero si en Distri')
                #print(distriDescData.loc[i])
            elif not dimesrow.empty and not (dimesrow['PORCENTAJEDESCUENTO'] == porcentajecliente).any():
                #print(dimesrow["PORCENTAJEDESCUENTO"])
                data.loc[len(data)] = {
                    "CLIENTEID": codigocliente,
                    "CODMARCA": codigomarca,
                    "DESCDIMES": dimesrow['PORCENTAJEDESCUENTO'].values[0],
                    "DESCDISTRI": porcentajecliente
                }
                
                #print('Existe en Dimes pero tienen un descuento distinto')
            

        data.to_csv("informe-diff-desc.csv", sep=',', index=False)

    except Exception as e:
        print(e)

                

if __name__ == '__main__':
    main()


