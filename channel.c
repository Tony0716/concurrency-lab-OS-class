#include "channel.h"
#include <semaphore.h>
//ZEhao
// Creates a new channel with the provided size and returns it to the caller
// A 0 size indicates an unbuffered channel, whereas a positive size indicates a buffered channel
channel_t* channel_create(size_t size)
{
    /* IMPLEMENT THIS */
    channel_t* channel = (channel_t*)malloc(sizeof(channel_t));
    //channel->buffer =  (buffer_t*)malloc(sizeof(buffer_t));
    channel->buffer = buffer_create(size);

    channel->isopen = true;
    pthread_mutex_init(&(channel->mutex),NULL);
    pthread_mutex_init(&(channel->closed),NULL);
    pthread_cond_init(&(channel->conrecv),NULL);
    pthread_cond_init(&(channel->consend),NULL);
    return channel;
}

// Writes data to the given channel
// This is a blocking call i.e., the function only returns on a successful completion of send
// In case the channel is full, the function waits till the channel has space to write the new data
// Returns SUCCESS for successfully writing data to the channel,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_send(channel_t *channel, void* data)
{   //locks the mutex
    pthread_mutex_lock(&(channel->mutex));
    // Wait the buffer is not full or the channel is closed. if the buffer is not full wait to send
    while (buffer_current_size(channel->buffer)>= buffer_capacity(channel->buffer))
    {
        pthread_mutex_lock(&(channel->closed));
        if (!channel->isopen)
        {
            pthread_mutex_unlock(&(channel->closed));
            pthread_mutex_unlock(&(channel->mutex));
            return CLOSED_ERROR;
        }
        pthread_mutex_unlock(&(channel->closed));
        pthread_cond_wait(&(channel->consend),&(channel->mutex));
    }
    // lock closed
    pthread_mutex_lock(&(channel->closed));
    // if open unlock closed and mutex
    if (!channel->isopen)
        {
            pthread_mutex_unlock(&(channel->closed));
            pthread_mutex_unlock(&(channel->mutex));
            return CLOSED_ERROR;
        }   
    pthread_mutex_unlock(&(channel->closed));
    // checks if the current size of the buffer is les than capacity for new data add in and add new data to buffer
    if (buffer_current_size(channel->buffer)< buffer_capacity(channel->buffer))
    {   
        if(buffer_add(channel->buffer,data) == BUFFER_ERROR){
            pthread_mutex_unlock(&(channel->mutex));
            return GEN_ERROR;
        }
        pthread_cond_broadcast(&(channel->conrecv));
        pthread_mutex_unlock(&(channel->mutex));
        return SUCCESS;
    }
    else{
        pthread_mutex_unlock(&(channel->mutex));
        return GEN_ERROR;
    }


    buffer_add(channel->buffer,data);
    pthread_cond_broadcast(&(channel->conrecv));
    pthread_mutex_unlock(&(channel->mutex));
    return SUCCESS;
}

// Reads data from the given channel and stores it in the function's input parameter, data (Note that it is a double pointer)
// This is a blocking call i.e., the function only returns on a successful completion of receive
// In case the channel is empty, the function waits till the channel has some data to read
// Returns SUCCESS for successful retrieval of data,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_receive(channel_t* channel, void** data)
{
    /* IMPLEMENT THIS */
    pthread_mutex_lock(&(channel->mutex));
    //to see if there are any data in buffer to be read.
    while (channel->buffer->size <= 0)
    //while (channel->buffer->capacity >= channel->buffer->size)
    {
        pthread_mutex_lock(&(channel->closed));
        if (!channel->isopen)
        {
            pthread_mutex_unlock(&(channel->closed));
            pthread_mutex_unlock(&(channel->mutex));
            return CLOSED_ERROR;
        }
        pthread_mutex_unlock(&(channel->closed));
        pthread_cond_wait(&(channel->conrecv),&(channel->mutex));

    }
    pthread_mutex_lock(&(channel->closed));
    if (!channel->isopen)
        {
            pthread_mutex_unlock(&(channel->closed));
            return CLOSED_ERROR;
        }
    pthread_mutex_unlock(&(channel->closed));
    buffer_remove(channel->buffer,data);
    pthread_cond_broadcast(&(channel->consend));
    pthread_mutex_unlock(&(channel->mutex));
    return SUCCESS;
}

// Writes data to the given channel
// This is a non-blocking call i.e., the function simply returns if the channel is full
// Returns SUCCESS for successfully writing data to the channel,
// CHANNEL_FULL if the channel is full and the data was not added to the buffer,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_non_blocking_send(channel_t* channel, void* data)
{
    /* IMPLEMENT THIS */
    pthread_mutex_lock(&(channel->mutex));
    if (buffer_current_size(channel->buffer)>= buffer_capacity(channel->buffer))
    {
        pthread_mutex_lock(&(channel->closed));
        if (!channel->isopen)
        {
            pthread_mutex_unlock(&(channel->closed));
            pthread_mutex_unlock(&(channel->mutex));
            return CLOSED_ERROR;
        }
        pthread_mutex_unlock(&(channel->closed));
        pthread_mutex_unlock(&(channel->mutex));
        return CHANNEL_FULL;
    }
    pthread_mutex_lock(&(channel->closed));
    if (!channel->isopen)
        {
            pthread_mutex_unlock(&(channel->closed));
            return CLOSED_ERROR;
        }    
    pthread_mutex_unlock(&(channel->closed));
    buffer_add(channel->buffer,data);
    pthread_cond_broadcast(&(channel->conrecv));
    pthread_mutex_unlock(&(channel->mutex));
    return SUCCESS;
}

// Reads data from the given channel and stores it in the function's input parameter data (Note that it is a double pointer)
// This is a non-blocking call i.e., the function simply returns if the channel is empty
// Returns SUCCESS for successful retrieval of data,
// CHANNEL_EMPTY if the channel is empty and nothing was stored in data,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_non_blocking_receive(channel_t* channel, void** data)
{
    /* IMPLEMENT THIS */
    pthread_mutex_lock(&(channel->mutex));
    if (channel->buffer->size == 0)
    //if (channel->buffer->capacity >= channel->buffer->size)
    {
        //pthread_mutex_lock(&(channel->closed));
        if (!channel->isopen)
        {
            //pthread_mutex_unlock(&(channel->closed));
            pthread_mutex_unlock(&(channel->mutex));
            return CLOSED_ERROR;
        }
        //pthread_mutex_unlock(&(channel->closed));
        pthread_mutex_unlock(&(channel->mutex));
        return CHANNEL_EMPTY;


    }
    //pthread_mutex_lock(&(channel->closed));
    if (!channel->isopen)
    {
        pthread_mutex_unlock(&(channel->closed));
        return CLOSED_ERROR;
    }
    //pthread_mutex_unlock(&(channel->closed));
    buffer_remove(channel->buffer,data);
    pthread_cond_broadcast(&(channel->consend));
    pthread_mutex_unlock(&(channel->mutex));
    return SUCCESS;
}

// Closes the channel and informs all the blocking send/receive/select calls to return with CLOSED_ERROR
// Once the channel is closed, send/receive/select operations will cease to function and just return CLOSED_ERROR
// Returns SUCCESS if close is successful,
// CLOSED_ERROR if the channel is already closed, and
// GEN_ERROR in any other error case
enum channel_status channel_close(channel_t* channel)
{
    /* IMPLEMENT THIS */
    pthread_mutex_lock(&(channel->closed));
    if (!channel->isopen)
        {
            pthread_mutex_unlock(&(channel->closed));
            return CLOSED_ERROR;
        }
    channel->isopen = false;
    pthread_cond_broadcast(&(channel->conrecv));
    pthread_cond_broadcast(&(channel->consend));
    pthread_mutex_unlock(&(channel->closed));
    return SUCCESS;
}

// Frees all the memory allocated to the channel
// The caller is responsible for calling channel_close and waiting for all threads to finish their tasks before calling channel_destroy
// Returns SUCCESS if destroy is successful,
// DESTROY_ERROR if channel_destroy is called on an open channel, and
// GEN_ERROR in any other error case
enum channel_status channel_destroy(channel_t* channel)
{
    /* IMPLEMENT THIS */
    pthread_mutex_lock(&(channel->closed));
    if (channel->isopen)
        {
            pthread_mutex_unlock(&(channel->closed));
            return DESTROY_ERROR;
        }
    buffer_free(channel->buffer);
    pthread_mutex_unlock(&(channel->closed));
    pthread_mutex_destroy(&(channel->mutex));
    pthread_mutex_destroy(&(channel->closed));
    pthread_cond_destroy(&(channel->conrecv));
    pthread_cond_destroy(&(channel->consend));
    free(channel);

    return SUCCESS;
}

// Takes an array of channels (channel_list) of type select_t and the array length (channel_count) as inputs
// This API iterates over the provided list and finds the set of possible channels which can be used to invoke the required operation (send or receive) specified in select_t
// If multiple options are available, it selects the first option and performs its corresponding action
// If no channel is available, the call is blocked and waits till it finds a channel which supports its required operation
// Once an operation has been successfully performed, select should set selected_index to the index of the channel that performed the operation and then return SUCCESS
// In the event that a channel is closed or encounters any error, the error should be propagated and returned through select
// Additionally, selected_index is set to the index of the channel that generated the error

enum channel_status channel_select(select_t* channel_list, size_t channel_count, size_t* selected_index)
{

    /* IMPLEMENT THIS */
   

       
    sem_t semaphore;
    sem_init(&semaphore, 0, 0);
    size_t blocking_channel_index = (size_t)-1;
    //First, try non-blocking operations
    for (size_t i = 0; i < channel_count; i++) {
        if (channel_list[i].dir == RECV) {
            enum channel_status recvstatus = channel_non_blocking_receive(channel_list[i].channel, &(channel_list[i].data));
            if (recvstatus == SUCCESS) {
                *selected_index = i;
                return SUCCESS;
            } 
            else if (recvstatus == CHANNEL_EMPTY) {
                blocking_channel_index = i; // Mark this channel for potential blocking
            } 
            else if (recvstatus != CLOSED_ERROR) {
                return recvstatus; 
            }
        } 
        else if (channel_list[i].dir == SEND) {
            enum channel_status send_status = channel_non_blocking_send(channel_list[i].channel, channel_list[i].data);
            if (send_status == SUCCESS) {
                *selected_index = i;
                return SUCCESS;
            } 
            else if (send_status == CHANNEL_FULL) {
                blocking_channel_index = i; 
            } 
            else if (send_status != CLOSED_ERROR) {
                return send_status; 
            }
        }
    }

    // If all non-blocking operations failed, try blocking operations
    if (blocking_channel_index != (size_t)-1) {

        sem_t sem;
        sem_init(&sem, 0, 0); // Initialize the semaphore with 0 value
        //sem_wait(&semaphore);
        // Attempt the blocking operation on the selected channel

        if (channel_list[blocking_channel_index].dir == RECV) {
            //sem_wait(&semaphore);
            enum channel_status recvstatus = channel_receive(channel_list[blocking_channel_index].channel, &(channel_list[blocking_channel_index].data));
            sem_post(&sem); // Unblock the semaphore
            if (recvstatus == SUCCESS) {
                *selected_index = blocking_channel_index;
                return SUCCESS;
            } 
            else if (recvstatus == CLOSED_ERROR) {
                return CLOSED_ERROR; 
            } 
            else {
                return GEN_ERROR;
            }
        } 
        else if (channel_list[blocking_channel_index].dir == SEND) {
            enum channel_status send_status = channel_send(channel_list[blocking_channel_index].channel, channel_list[blocking_channel_index].data);
            sem_post(&sem); 
            if (send_status == SUCCESS) {
                *selected_index = blocking_channel_index;
                return SUCCESS;
            } 
            else if (send_status == CLOSED_ERROR) {
                return CLOSED_ERROR;
            }
            else {
                return GEN_ERROR;
            }
        }
    }

    // If all channels are closed or some other error
    return GEN_ERROR;

}
