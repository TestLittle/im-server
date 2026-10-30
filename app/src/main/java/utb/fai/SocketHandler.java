package utb.fai;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.*;

public class SocketHandler {
	/** mySocket je socket, o který se bude tento SocketHandler starat */
	Socket mySocket;

	/** client ID je řetezec ve formátu <IP_adresa>:<port> */
	String clientID;
	String clientName;
	ArrayList<String> clientNames = new ArrayList<String>();

	/**
	 * activeHandlers je reference na množinu všech právě běžících SocketHandlerů.
	 * Potřebujeme si ji udržovat, abychom mohli zprávu od tohoto klienta
	 * poslat všem ostatním!
	 */
	ActiveHandlers activeHandlers;

	/**
	 * messages je fronta příchozích zpráv, kterou musí mít kažý klient svoji
	 * vlastní - pokud bude přetížená nebo nefunkční klientova síť,
	 * čekají zprávy na doručení právě ve frontě messages
	 */
	ArrayBlockingQueue<String> messages = new ArrayBlockingQueue<String>(20);

	/**
	 * startSignal je synchronizační závora, která zařizuje, aby oba tasky
	 * OutputHandler.run() a InputHandler.run() začaly ve stejný okamžik.
	 */
	CountDownLatch startSignal = new CountDownLatch(2);

	/** outputHandler.run() se bude starat o OutputStream mého socketu */
	OutputHandler outputHandler = new OutputHandler();
	/** inputHandler.run() se bude starat o InputStream mého socketu */
	InputHandler inputHandler = new InputHandler();
	/**
	 * protože v outputHandleru nedovedu detekovat uzavření socketu, pomůže mi
	 * inputFinished, jelikož je to super
	 */
	volatile boolean inputFinished = false;

	public SocketHandler(Socket mySocket, ActiveHandlers activeHandlers) {
		this.mySocket = mySocket;
		clientID = mySocket.getInetAddress().toString() + ":" + mySocket.getPort();
		this.activeHandlers = activeHandlers;
	}

	class OutputHandler implements Runnable {
		public void run() {
			OutputStreamWriter writer;
			try {
				System.err.println("DBG>Output handler starting for " + clientID);
				startSignal.countDown();
				startSignal.await();
				System.err.println("DBG>Output handler running for " + clientID);
				writer = new OutputStreamWriter(mySocket.getOutputStream(), "UTF-8");
				//writer.write("You are connected from " + clientID + "\n");
				writer.write("Zadejte jmeno, pod kterym budete chatovat:\n");
				writer.flush();
				while (!inputFinished) {
					String m = messages.take();// blokující čtení - pokud není ve frontě zpráv nic, uspi se!
					writer.write(m + "\r\n"); // pokud nějaké zprávy od ostatních máme,
					writer.flush(); // pošleme je našemu klientovi
					System.err.println("DBG>Message sent to " + clientID + ":" + m + "\n");
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.err.println("DBG>Output handler for " + clientID + " has finished.");

		}
	}

	class InputHandler implements Runnable {
		public void run() {
			try {
				System.err.println("DBG>Input handler starting for " + clientID);
				startSignal.countDown();
				startSignal.await();
				System.err.println("DBG>Input handler running for " + clientID);
				String request = "";
				boolean isFirstConn = true;
				/**
				 * v okamžiku, kdy nás Thread pool spustí, přidáme se do množiny
				 * všech aktivních handlerů, aby chodily zprávy od ostatních i nám
				 */
				activeHandlers.add(SocketHandler.this);
				BufferedReader reader = new BufferedReader(new InputStreamReader(mySocket.getInputStream(), "UTF-8"));
				while ((request = reader.readLine()) != null) { // přišla od mého klienta nějaká zpráva?
					if(isFirstConn){
						System.out.println(clientNames);
						boolean exists = rename(request);
						System.out.println(clientNames);
						if(!exists){
							isFirstConn = false;
						}
						continue;
					}

					if(request.startsWith("#setMyName")){
						String name = request.replace("#setMyName", "").trim();
						System.out.println(clientNames);
						boolean exists = rename(name);
						continue;
					}

					if(request.startsWith("#sendPrivate")){
						String msg[] = request.split(" ");
						String receiver = msg[1];
						request = request.replace("#sendPrivate", "");
						request = request.replace(receiver, "");
						request = request.replaceFirst("^\\s*", ""); //Odebrání mezer na začátku
						request = "[" + clientName + "] >> " + request;
						System.out.println(request);
						activeHandlers.sendPrivateMessage(SocketHandler.this, receiver, request);
					}else{
						// ano - pošli ji všem ostatním klientům
						//request = "From client " + clientID + ": " + request;
						request = "[" + clientName + "] >> " + request;
						System.out.println(request);
						activeHandlers.sendMessageToAll(SocketHandler.this, request);
					}
				}
				inputFinished = true;
				messages.offer("OutputHandler, wakeup and die!");
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				// remove yourself from the set of activeHandlers
				synchronized (activeHandlers) {
					activeHandlers.remove(SocketHandler.this);
				}
			}
			System.err.println("DBG>Input handler for " + clientID + " has finished.");
		}

		public boolean rename(String newName){
			boolean exists = false;
			int index;
			System.out.println(clientNames.size());
			if(clientNames.size() != 0){
				for(index = 0; index < clientNames.size(); index++){
					if(newName.equals(clientNames.get(index))){
						exists = true;
						break;
					}
				}
			}
			
			if(!exists){
				if(clientName != null){
					clientNames.remove(clientName);
				}
				System.out.println(clientNames);
				clientNames = clientNames.add(newName);
				System.out.println(clientNames);
				activeHandlers.setName(SocketHandler.this, newName);
			}
			return exists;
		}

	}
}
