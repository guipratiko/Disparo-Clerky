/**
 * Script para executar migration SQL
 */

import { readFileSync } from 'fs';
import { join } from 'path';
import { pgPool } from '../config/databases';

const runMigration = async () => {
  try {
    console.log('🔄 Executando migrations de disparos...\n');

    const client = await pgPool.connect();

    try {
      // Executar migrations em ordem
      const migrations = [
        '006_create_dispatches_tables.sql',
        '007_add_instance_name_to_dispatches.sql',
        '008_add_user_timezone_to_dispatches.sql',
      ];

      for (const migrationFile of migrations) {
        const migrationPath = join(__dirname, `../database/migrations/${migrationFile}`);
        const sql = readFileSync(migrationPath, 'utf-8');
        
        console.log(`\n📝 Executando migration: ${migrationFile}`);
        
        try {
          await client.query(sql);
          console.log(`✅ Migration ${migrationFile} executada com sucesso`);
        } catch (error: any) {
          // Ignorar erros de "já existe" (IF NOT EXISTS)
          if (error.message && (
            error.message.includes('already exists') ||
            error.message.includes('duplicate key') ||
            error.message.includes('relation already exists') ||
            (error.message.includes('column') && error.message.includes('already exists'))
          )) {
            console.log(`⚠️  Algumas alterações já existem em ${migrationFile}, mas isso é normal.`);
          } else {
            throw error;
          }
        }
      }
      
      console.log('\n✅ Todas as migrations executadas com sucesso!');
    } finally {
      client.release();
    }

    await pgPool.end();
    process.exit(0);
  } catch (error: any) {
    console.error('❌ Erro ao executar migration:', error.message);
    if (error.code) {
      console.error('   Código PostgreSQL:', error.code);
    }
    process.exit(1);
  }
};

runMigration();
