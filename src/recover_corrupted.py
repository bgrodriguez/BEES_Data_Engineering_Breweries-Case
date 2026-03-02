#!/usr/bin/env python
"""
Script para diagnosticar e tentar recuperar dados de arquivos parquet corrompidos.
Estratégias:
1. Tentar ler linha por linha
2. Tentar ler por row_groups
3. Extrair metadados
4. Se houver sucesso parcial, exportar dados recuperáveis
"""

import sys
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

def diagnose_corrupted(filepath):
    """Tenta extrair máximo de informação possível de um arquivo corrompido."""
    
    fp = Path(filepath)
    if not fp.exists():
        print(f"❌ Arquivo não existe: {fp}")
        return
    
    print(f"\n📋 Diagnosticando: {fp}")
    print(f"   Tamanho: {fp.stat().st_size / 1024:.2f} KB")
    
    # Estratégia 1: Tentar ler como parquet file objeto
    try:
        pf = pq.ParquetFile(fp)
        print(f"✓ Metadados parquet lidos")
        print(f"  - Num row groups: {pf.num_row_groups}")
        print(f"  - Schema: {pf.schema}")
        
        # Estratégia 2: Tentar ler cada row group
        recovered_data = []
        for i in range(pf.num_row_groups):
            try:
                table = pf.read_row_group(i)
                df = table.to_pandas()
                print(f"  ✓ Row group {i}: {len(df)} linhas lidas")
                recovered_data.append(df)
            except Exception as e:
                print(f"  ✗ Row group {i}: falha - {e}")
        
        if recovered_data:
            combined = pd.concat(recovered_data, ignore_index=True)
            print(f"\n✓ Recuperados {len(combined)} registros do arquivo")
            return combined
        else:
            print(f"\n✗ Nenhum row group pôde ser lido")
            return None
            
    except Exception as e:
        print(f"✗ Erro ao ler arquivo: {e}")
        
        # Estratégia 3: Tentar com engines alternativos
        try:
            print("\n  Tentando com engine 'fastparquet'...")
            df = pd.read_parquet(fp, engine='fastparquet')
            print(f"  ✓ Lido com fastparquet: {len(df)} linhas")
            return df
        except:
            print(f"  ✗ fastparquet também falhou")
            return None

def main():
    if len(sys.argv) < 2:
        print("Uso: python recover_corrupted.py <filepath>")
        print("\nExemplo:")
        print("  python recover_corrupted.py 'data/silver/country=United%20States/state=Missouri/c625b099664848fc95ab3977706596e8-0.parquet'")
        sys.exit(1)
    
    filepath = sys.argv[1]
    recovered_df = diagnose_corrupted(filepath)
    
    if recovered_df is not None and len(recovered_df) > 0:
        # Opcionalmente salvar os dados recuperados
        recovery_path = Path(filepath).parent / Path(filepath).stem + "_recovered.parquet"
        try:
            recovered_df.to_parquet(recovery_path)
            print(f"\n💾 Dados recuperados salvos em: {recovery_path}")
        except Exception as e:
            print(f"\n⚠ Não fue possível salvar recuperação: {e}")
    else:
        print(f"\n⚠ Arquivo irrecuperável - considere reprocessar os dados do bronze")

if __name__ == "__main__":
    main()
