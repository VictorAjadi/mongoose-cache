
import * as fs from 'node:fs';
import * as path from 'node:path';
import { execSync } from 'node:child_process';
import * as readline from 'node:readline';

/**
 * ============================================================================
 * Cleanup & Removal Script
 * ============================================================================
 * 
 * WARNING: This script can remove node_modules and configuration files.
 */

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

const question = (query: string): Promise<string> => 
    new Promise((resolve) => rl.question(query, resolve));

async function runCleanup() {
    console.log('\n🗑️  @mongoose-cache Cleanup Utility');
    console.log('---------------------------------');

    const confirm = await question('Are you sure you want to remove node_modules and lock files? (y/N): ');
    
    if (confirm.toLowerCase() === 'y') {
        const pathsToRemove = [
            'node_modules',
            'package-lock.json',
            'dist',
            'bun.lockb'
        ];

        for (const p of pathsToRemove) {
            const fullPath = path.join(process.cwd(), p);
            if (fs.existsSync(fullPath)) {
                console.log(`Removing ${p}...`);
                fs.rmSync(fullPath, { recursive: true, force: true });
            }
        }
        
        const deletePkg = await question('Delete package.json as well? (DANGEROUS) (y/N): ');
        if (deletePkg.toLowerCase() === 'y') {
            fs.unlinkSync(path.join(process.cwd(), 'package.json'));
            console.log('Removed package.json');
        }

        console.log('\n✅ Cleanup complete. The directory is now clean.');
    } else {
        console.log('Cleanup cancelled.');
    }

    rl.close();
}

runCleanup();
