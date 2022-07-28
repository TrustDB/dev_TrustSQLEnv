/* Copyright (c) 2020, TRUSTDB Inc.
   
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.   

   You should have received a copy of the GNU General Public License
   along with this program; If not, see <http://www.gnu.org/licenses/>.
*/

package org.rdlms.wallet;

import java.io.Console;
import java.util.HashMap;

public class WalletManager {
    private HashMap<String,ECDSAWallet> hmAllWallets = new HashMap<String,ECDSAWallet>();

    /**
     * read wallet file
     * @param walletName
     * @return
     */
    public String readWallet(String walletName) {
        ECDSAWallet wallet; 
        String account;

        wallet= new ECDSAWallet();
        account = wallet.read(walletName);

        
        if(hmAllWallets.containsKey(walletName)) {
            hmAllWallets.replace(walletName, wallet);
        } else {
            hmAllWallets.put(walletName, wallet);
        }
        
        return account;
    }

    /**
     * read wallet file and decrypt private key and keeping it.
     * @param walletName
     * @param password
     * @return
     */

    public String readWallet(String walletName, String password) {
        ECDSAWallet wallet; 
        String account;

        wallet= new ECDSAWallet();
        account = wallet.read(walletName, password);

        if(hmAllWallets.containsKey(walletName)) {
            hmAllWallets.replace(walletName, wallet);
        } else {
            hmAllWallets.put(walletName, wallet);
        }
        return account;
    }

     /**
     * read account (compressed public key) from ECDSAobject in hashmap
     * @param walletName
     * @param password
     * @return
     */
    public String getAccount(String walletName) {
        ECDSAWallet wallet; 
        String account;

        wallet= hmAllWallets.get(walletName);
        account = wallet.getAccount();
        return account;
    }


    /**
     * sign with ECDSAWallet don't keeping private key.
     * @param walletName
     * @param inputs
     * @return
     */
    public String secureSign(String walletName, String inputs) {
        String strSign;
        ECDSAWallet wallet=null;
        String password=null;
        Console cons = System.console();
        
        try {
            wallet = hmAllWallets.get(walletName);
            if(wallet==null) return null;
            System.out.println("");
            System.out.println("[WalletManger]=============================================");
            System.out.println("!!  Enter password to access the wallet ["+walletName+"]");            
            password = new String(cons.readPassword());
            System.out.println("============================================================");            
            System.out.println("");
            strSign = wallet.sign(inputs, password);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return null;
        }
        return strSign;
    }

    /**
     * sign with ECDSAWallet keeping Private Key.
     * @param walletName
     * @param inputs
     * @return
     */
    public String sign(String walletName, String inputs) {
        String strSign;
        ECDSAWallet wallet=null;        
        try {
            wallet = hmAllWallets.get(walletName);
            if(wallet==null) return null;            
            strSign = wallet.sign(inputs);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return null;
        }
        return strSign;
    }
    
    public static void main(String args[]) {
        String name, password;
        Console cons = System.console();                
        String mission=null;				

        while(true) {
            System.out.println("\n"); 
            System.out.println("==========================================================");  
            System.out.println("||                                                      ||"); 
            System.out.println("||          Wallet Manager (ECDSA,secp256k1)            ||");  
            System.out.println("||          --------------------------------            ||");
            System.out.println("||                                                      ||"); 
            System.out.println("||                    Copyright (c) 2020, TRUSTDB Inc.  ||");		
            System.out.println("==========================================================");  
            System.out.println("\n\n");
                    
            System.out.println("\n");
            System.out.println("Waht do you want to do ? ( 0: Generate a new wallet  1: Read Wallet )");
            mission = cons.readLine();
            
            if(mission.equals("0")) {	
                while(true) {							
                    System.out.println("Enter the name of a new wallet (got to back 'quit')"); 
                    name = cons.readLine();                                        
                    if(name.equals("quit")) break;		
                    if(name.equals("")) continue;
                               
                    System.out.println("\n");
                    System.out.println("Enter password to secure the wallet");                                         
                    password = new String(cons.readPassword());
                                    
                    ECDSAWallet wallet = new ECDSAWallet();
                    String account = wallet.generate(name, password);

                    if(account==null) {
                        System.out.println("Fail to generate a new wallet");
                        break;
                    }
                    
                    try{
                        wallet.write();
                    } catch (Exception e) {
                        System.out.println("Fail to generate a new wallet!");    
                        System.out.println(e.getMessage());
                    }
                    
                    System.out.println("Wallet ["+name+"] generated successfully!");
                    System.out.println("");
                }
            } else if(mission.equals("1")) {
                while(true) {							
                    System.out.println("Enter the name of wallet to read (got to back 'quit')"); 
                    name = cons.readLine();                                        
                    if(name.equals("quit")) break;		
                    if(name.equals("")) continue;
                               
                    System.out.println("\n");
                    System.out.println("Enter password to secure the wallet");                                         
                    password = new String(cons.readPassword());
                    System.out.println("\n");
                
                    ECDSAWallet wallet = new ECDSAWallet();
                    String account = wallet.read(name);

                    if(account==null) {
                        System.out.println("Fail to read the wallet");
                        break;
                    }                    
                    System.out.println("Wallet ["+name+"] has account ="+account);
                    System.out.println("");
                }		
            } else if(mission.equals("999")) {    
                System.out.println("How many wallets you want to make? password is \"test\""); 
                String countStr = cons.readLine();                
                ECDSAWallet wallet = new ECDSAWallet();
                String account;
                for(int i=1; i<Integer.parseInt(countStr)+1; i++) {
                    name = "TEST_#"+Integer.toString(i);
                    System.out.println("Generates.. "+name);
                    wallet = new ECDSAWallet();
                    account = wallet.generate(name, "test");
                    if(account==null) {
                        System.out.println("Fail to generate a new wallet");
                        break;
                    }                    
                    try{
                        wallet.write();
                    } catch (Exception e) {
                        System.out.println("Fail to generate a new wallet!");    
                        System.out.println(e.getMessage());
                        break;
                    }
                } 
            }
            break;
        }				
    }
    
}